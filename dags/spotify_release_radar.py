"""Spotify Release Radar.

Checks the connected Spotify account's followed artists for new
albums/singles, records them in Postgres, and emails a summary when new
releases are found.

Failure alerting: every task in this DAG runs with
include.alerting.on_failure_callback (see default_args below), which sends a
structured failure email and writes an audit row to spotify_radar.alert_log.
"""

from __future__ import annotations

import os
import time

import pendulum
from airflow.decorators import dag, task
from airflow.utils.email import send_email

from include import db, spotify_client
from include.alerting import on_failure_callback

default_args = {
    "owner": "spotify-radar",
    "retries": 1,
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
    def sync_followed_artists() -> list[dict]:
        """Refresh the list of tracked artists from Spotify."""
        access_token = spotify_client.get_access_token()
        artists = spotify_client.get_followed_artists(access_token)
        db.upsert_tracked_artists(artists)
        return artists

    @task
    def fetch_and_diff_releases(artists: list[dict]) -> list[dict]:
        """Find releases for each tracked artist that we haven't seen before."""
        access_token = spotify_client.get_access_token()
        new_releases = []

        for i, artist in enumerate(artists):
            if i > 0:
                # Spread requests out to stay comfortably under Spotify's
                # per-app rate limit -- a burst of one request per artist
                # can trigger a long (~24h) lockout for apps in Development Mode.
                time.sleep(0.3)
            albums = spotify_client.get_artist_albums(access_token, artist["id"])
            seen_ids = db.get_seen_release_ids(artist["id"])
            for album in spotify_client.diff_new_releases(albums, seen_ids):
                new_releases.append(
                    {**album, "artist_id": artist["id"], "artist_name": artist["name"]}
                )

        return new_releases

    @task
    def load_new_releases(new_releases: list[dict]) -> list[dict]:
        """Persist newly discovered releases and pass them through."""
        db.insert_new_releases(new_releases)
        return new_releases

    @task
    def notify_new_releases(new_releases: list[dict]) -> None:
        """Email a summary if any new releases were found."""
        if not new_releases:
            print("No new releases found.")
            return

        rows = "".join(
            f"<tr><td>{r['artist_name']}</td><td>{r['name']}</td>"
            f"<td>{r['album_type']}</td><td>{r['release_date']}</td></tr>"
            for r in new_releases
        )
        html_content = f"""
        <h2>New releases from artists you follow</h2>
        <table border="1" cellpadding="6">
            <tr><th>Artist</th><th>Title</th><th>Type</th><th>Release date</th></tr>
            {rows}
        </table>
        """
        send_email(
            to=os.environ["ALERT_EMAIL_TO"],
            subject=f"\U0001F3B5 {len(new_releases)} new Spotify release(s)",
            html_content=html_content,
        )

    artists = sync_followed_artists()
    new_releases = fetch_and_diff_releases(artists)
    loaded = load_new_releases(new_releases)
    notify_new_releases(loaded)


spotify_release_radar()
