"""Release Radar pipeline steps, called by dags/spotify_release_radar.py.

Kept free of Airflow imports so each step is unit testable with mocked
`db`, `spotify_client`, and `send_email`.

The pipeline fans out over users but shares work across them:

1. sync_users (per user batch): refresh each user's follow list and
   listening-history affinity, if stale.
2. discover_releases (once): check each followed artist for new albums,
   one API call per artist regardless of how many users follow it.
3. deliver_digests (per user batch): rank the user's not-yet-sent releases
   into today's digest and send it, exactly once per user per day.
"""

from __future__ import annotations

import html
import logging
import os
from datetime import date, datetime, timedelta, timezone

import requests

from include import db, ranking, spotify_client

log = logging.getLogger(__name__)

TOP_ARTIST_TIME_RANGES = ("short_term", "medium_term")
# Releases dated more than this many days before the digest date are never
# emailed, even if they look new to us (reissues, back catalog surfacing).
DEFAULT_MAX_RELEASE_AGE_DAYS = 30


class BatchFailed(Exception):
    """Some users in a batch failed; the rest were processed."""


def bootstrap_env_user() -> None:
    """Register the single-user setup from .env as the first user.

    Lets a project configured before multi-user support keep working with
    no manual migration: if no users exist yet but SPOTIFY_REFRESH_TOKEN
    and ALERT_EMAIL_TO are set, that account becomes a user.
    """
    refresh_token = os.environ.get("SPOTIFY_REFRESH_TOKEN")
    email = os.environ.get("ALERT_EMAIL_TO")
    if not (refresh_token and email) or db.count_users() > 0:
        return

    token = spotify_client.refresh_user_token(refresh_token)
    me = spotify_client.get_current_user(token.access_token)
    db.upsert_user(me["id"], me["display_name"], email, token.refresh_token or refresh_token)
    log.info("Registered %s from SPOTIFY_REFRESH_TOKEN as the first user.", me["id"])


def _is_stale(synced_at: datetime | None, max_age_hours: float, now: datetime) -> bool:
    return synced_at is None or now - synced_at >= timedelta(hours=max_age_hours)


def _check_cooldown() -> bool:
    """True if Spotify calls are currently allowed."""
    remaining = db.get_cooldown_remaining()
    if remaining > 0:
        log.warning("Spotify rate-limit cooldown active for another %.0fs; skipping API calls.", remaining)
        return False
    return True


def sync_users(user_ids: list[str], follows_max_age_hours: float, affinity_max_age_hours: float) -> dict:
    """Refresh follow lists and listening-history affinity for a batch of users.

    Each is skipped if synced within its max age, so re-runs and manual
    triggers don't repeat API calls. One user's failure (e.g. a revoked
    token) doesn't stop the batch; failures are raised together at the end.
    A rate limit stops the batch immediately.
    """
    stats = {"synced": 0, "skipped": 0, "failed": []}
    if not _check_cooldown():
        return stats

    now = datetime.now(timezone.utc)
    for user in db.get_users(user_ids):
        sync_follows = _is_stale(user["follows_synced_at"], follows_max_age_hours, now)
        sync_affinity = _is_stale(user["affinity_synced_at"], affinity_max_age_hours, now)
        if not (sync_follows or sync_affinity):
            stats["skipped"] += 1
            continue

        try:
            _sync_user(user, sync_follows, sync_affinity)
            stats["synced"] += 1
        except spotify_client.RateLimitError as e:
            db.set_cooldown(e.retry_after)
            log.warning("Rate limited while syncing %s (retry after %ds); stopping batch.", user["user_id"], e.retry_after)
            break
        except Exception:
            log.exception("Failed to sync user %s", user["user_id"])
            stats["failed"].append(user["user_id"])

    log.info("User sync: %s", stats)
    if stats["failed"]:
        raise BatchFailed(f"Failed to sync users: {stats['failed']}")
    return stats


def _sync_user(user: dict, sync_follows: bool, sync_affinity: bool) -> None:
    token = spotify_client.refresh_user_token(user["refresh_token"])
    if token.refresh_token and token.refresh_token != user["refresh_token"]:
        db.update_refresh_token(user["user_id"], token.refresh_token)

    if sync_follows:
        artists = spotify_client.get_followed_artists(token.access_token)
        db.replace_user_follows(user["user_id"], artists)

    if sync_affinity:
        try:
            top = {
                time_range: spotify_client.get_top_artist_ids(token.access_token, time_range)
                for time_range in TOP_ARTIST_TIME_RANGES
            }
        except requests.HTTPError as e:
            if e.response is None or e.response.status_code != 403:
                raise
            # Token predates the user-top-read scope: rank without history
            # (and don't retry every run) until the user re-authorizes.
            log.warning("User %s hasn't granted user-top-read; digest won't be personalized.", user["user_id"])
            top = {}
        db.replace_user_affinity(user["user_id"], ranking.affinity_from_top_artists(top))


def discover_releases(batch_size: int, min_recheck_hours: float) -> dict:
    """Check up to `batch_size` due artists for releases not seen before.

    Each artist is checked once with the shared app token, however many
    users follow it: one request for albums and one for singles, each with
    If-None-Match so an unchanged discography costs a 304. The first check of an artist records a baseline that's
    never notified. Each artist's results commit on their own, so an
    interrupted run keeps its progress.
    """
    stats = {"checked": 0, "not_modified": 0, "new_releases": 0, "rate_limited": False}
    if not _check_cooldown():
        stats["rate_limited"] = True
        return stats

    artists = db.get_artists_to_check(batch_size, min_recheck_hours)
    if not artists:
        log.info("No artists due for a release check.")
        return stats

    token = spotify_client.get_app_access_token()
    for artist in artists:
        try:
            result = spotify_client.get_artist_releases(token, artist["id"], etags=artist["etags"])
        except spotify_client.RateLimitError as e:
            # Stop rather than keep calling a rate-limited API. Artists not
            # checked stay at the front of the queue for the next run.
            db.set_cooldown(e.retry_after)
            log.warning(
                "Spotify rate limit hit after %d/%d artists (retry after %ds); stopping early.",
                stats["checked"], len(artists), e.retry_after,
            )
            stats["rate_limited"] = True
            break

        if result.albums is None:
            db.record_artist_check(artist["id"], [], is_baseline=False, etags=result.etags)
            stats["not_modified"] += 1
        else:
            seen_ids = db.get_seen_release_ids(artist["id"])
            new = spotify_client.diff_new_releases(result.albums, seen_ids)
            # No history yet means this is the first check: baseline, no alert.
            db.record_artist_check(artist["id"], new, is_baseline=not seen_ids, etags=result.etags)
            if seen_ids:
                stats["new_releases"] += len(new)
        stats["checked"] += 1

    log.info("Release discovery: %s", stats)
    return stats


def deliver_digests(
    user_ids: list[str],
    digest_date: date,
    send_email,
    max_release_age_days: int = DEFAULT_MAX_RELEASE_AGE_DAYS,
) -> dict:
    """Build and send each user's digest for `digest_date`, at most once.

    Idempotent per (user, digest_date): pending releases are attached to
    that day's digest (each album goes into at most one digest per user),
    and a digest is claimed before sending, so re-running the day finds it
    already sent. Older digests left unsent by a failed run go out too.
    Only releases dated within `max_release_age_days` of `digest_date`
    qualify, measured from the run date rather than today so a re-run
    makes the same decision.
    """
    released_since = digest_date - timedelta(days=max_release_age_days)
    stats = {"sent": 0, "failed": []}
    for user in db.get_users(user_ids):
        user_id = user["user_id"]
        try:
            pending = db.get_pending_releases(user_id, released_since)
            if pending:
                ranked = ranking.rank_releases(pending, db.get_user_affinity(user_id), digest_date)
                db.add_digest_items(user_id, digest_date, ranked)

            for pending_date in db.get_pending_digest_dates(user_id, up_to=digest_date):
                if _send_digest(user, pending_date, send_email):
                    stats["sent"] += 1
        except Exception:
            log.exception("Failed to deliver digest for user %s", user_id)
            stats["failed"].append(user_id)

    log.info("Digest delivery: %s", stats)
    if stats["failed"]:
        raise BatchFailed(f"Failed to deliver digests for users: {stats['failed']}")
    return stats


def _send_digest(user: dict, digest_date: date, send_email) -> bool:
    user_id = user["user_id"]
    if not db.claim_digest(user_id, digest_date):
        return False  # another attempt already claimed or sent it

    items = db.get_digest_items(user_id, digest_date)
    if not items:
        db.mark_digest_sent(user_id, digest_date)
        return False

    try:
        send_email(
            to=user["email"],
            subject=f"\U0001F3B5 {len(items)} new Spotify release(s)",
            html_content=render_digest(items),
        )
    except Exception:
        # The send raised, so treat the email as not delivered and let a
        # retry pick it up. If the process dies between a successful send
        # and mark_digest_sent, the digest stays 'sending' and is never
        # resent -- deliberately erring toward at-most-once.
        db.release_digest_claim(user_id, digest_date)
        raise
    db.mark_digest_sent(user_id, digest_date)
    return True


def render_digest(items: list[dict]) -> str:
    rows = "".join(
        f"<tr><td>{html.escape(', '.join(item['artist_names']))}</td>"
        f"<td>{html.escape(item['album_name'])}</td>"
        f"<td>{html.escape(item['album_type'])}</td><td>{item['release_date']}</td></tr>"
        for item in items
    )
    return f"""
    <h2>New releases from artists you follow</h2>
    <p>Ordered by how much you've been listening to each artist.</p>
    <table border="1" cellpadding="6">
        <tr><th>Artist</th><th>Title</th><th>Type</th><th>Release date</th></tr>
        {rows}
    </table>
    """
