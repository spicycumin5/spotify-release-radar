"""Spotify Release Radar.

Emails each registered user a daily digest of new albums/singles from the
artists they follow, ordered by their listening history.

    list_user_batches
          |
    sync_users.expand(batch)      per-user follows + top artists (if stale)
          |
    discover_releases             once per followed artist, shared by all users
          |
    deliver_digests.expand(batch) rank + send each user's digest for {{ ds }}

Work fans out over batches of users (USER_BATCH_SIZE each) via dynamic task
mapping, but the expensive part -- checking artists for new releases --
runs once per distinct artist across all users. A run checks at most
ARTIST_BATCH_SIZE artists, rotating through the longest-overdue ones, to
stay under Spotify's Development Mode rate limit.

Idempotency: digests are keyed by (user, ds) and claimed before sending,
and an album can only land in one digest per user, so clearing or
re-running a day never sends anyone a second email. See include/radar.py.

Failure alerting: every task in this DAG runs with
include.alerting.on_failure_callback (see default_args below), which sends a
structured failure email and writes an audit row to spotify_radar.alert_log.
"""

from __future__ import annotations

import os

import pendulum
from airflow.decorators import dag, task
from airflow.utils.email import send_email
from airflow.utils.trigger_rule import TriggerRule

from include import db, radar
from include.alerting import on_failure_callback

# Max artists to check for new releases per run (two Spotify calls each:
# albums and singles).
ARTIST_BATCH_SIZE = int(os.environ.get("SPOTIFY_ARTIST_BATCH_SIZE", "20"))
# Skip artists checked more recently than this, so re-runs are free.
ARTIST_MIN_RECHECK_HOURS = float(os.environ.get("SPOTIFY_ARTIST_MIN_RECHECK_HOURS", "20"))
# Users per mapped task instance.
USER_BATCH_SIZE = int(os.environ.get("RADAR_USER_BATCH_SIZE", "25"))
# How often to re-fetch each user's follow list and top artists.
FOLLOWS_MAX_AGE_HOURS = float(os.environ.get("RADAR_FOLLOWS_MAX_AGE_HOURS", "20"))
AFFINITY_MAX_AGE_HOURS = float(os.environ.get("RADAR_AFFINITY_MAX_AGE_HOURS", "72"))
# Never email releases dated more than this many days before the run date.
MAX_RELEASE_AGE_DAYS = int(os.environ.get("RADAR_MAX_RELEASE_AGE_DAYS", "30"))

default_args = {
    "owner": "spotify-radar",
    # Retrying a Spotify call right after a 429 is known to push the
    # lockout window back out (sometimes to ~24h), so these tasks don't
    # get automatic Airflow retries. Transient errors are retried with
    # backoff inside include/spotify_client.py instead.
    "retries": 0,
    "on_failure_callback": on_failure_callback,
}


@dag(
    dag_id="spotify_release_radar",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["spotify", "alerting-demo"],
)
def spotify_release_radar():
    @task
    def list_user_batches() -> list[list[str]]:
        """Split active users into batches to fan out over (user ids only -- tokens stay in Postgres)."""
        radar.bootstrap_env_user()
        user_ids = db.get_active_user_ids()
        return [user_ids[i : i + USER_BATCH_SIZE] for i in range(0, len(user_ids), USER_BATCH_SIZE)]

    # One batch at a time: Spotify's rate limit is per app, so parallel
    # batches would just burst into it.
    @task(max_active_tis_per_dagrun=1)
    def sync_users(user_ids: list[str]) -> dict:
        return radar.sync_users(user_ids, FOLLOWS_MAX_AGE_HOURS, AFFINITY_MAX_AGE_HOURS)

    # Runs even if some user batches failed to sync -- the other users'
    # artists (and the failed users' last-known follows) still need checking.
    @task(trigger_rule=TriggerRule.ALL_DONE)
    def discover_releases() -> dict:
        return radar.discover_releases(ARTIST_BATCH_SIZE, ARTIST_MIN_RECHECK_HOURS)

    # Runs even if discovery failed: releases found by earlier runs can
    # still be delivered.
    @task(trigger_rule=TriggerRule.ALL_DONE, max_active_tis_per_dagrun=4)
    def deliver_digests(user_ids: list[str], ds: str | None = None) -> dict:
        return radar.deliver_digests(user_ids, pendulum.parse(ds).date(), send_email, MAX_RELEASE_AGE_DAYS)

    batches = list_user_batches()
    synced = sync_users.expand(user_ids=batches)
    discovered = discover_releases()
    delivered = deliver_digests.expand(user_ids=batches)
    synced >> discovered >> delivered


spotify_release_radar()
