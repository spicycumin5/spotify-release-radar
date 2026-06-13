"""Postgres helpers for the spotify_radar schema (see include/sql/schema.sql).

Every function accepts an optional DB-API `conn` so it can be unit tested
with a mock connection. When omitted, a connection is pulled from Airflow's
`postgres_default` connection via PostgresHook.
"""

from __future__ import annotations


def _get_conn():
    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id="postgres_default").get_conn()


def upsert_tracked_artists(artists: list[dict], conn=None) -> None:
    """Insert/update tracked artists, stamping last_checked_at = now()."""
    own_conn = conn is None
    conn = conn or _get_conn()
    try:
        with conn.cursor() as cur:
            for artist in artists:
                cur.execute(
                    """
                    INSERT INTO spotify_radar.tracked_artists (artist_id, artist_name, last_checked_at)
                    VALUES (%s, %s, now())
                    ON CONFLICT (artist_id) DO UPDATE
                        SET artist_name = EXCLUDED.artist_name,
                            last_checked_at = EXCLUDED.last_checked_at
                    """,
                    (artist["id"], artist["name"]),
                )
        conn.commit()
    finally:
        if own_conn:
            conn.close()


def get_seen_release_ids(artist_id: str, conn=None) -> set[str]:
    """Return album_ids already recorded for this artist."""
    own_conn = conn is None
    conn = conn or _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT album_id FROM spotify_radar.artist_releases WHERE artist_id = %s",
                (artist_id,),
            )
            return {row[0] for row in cur.fetchall()}
    finally:
        if own_conn:
            conn.close()


def insert_new_releases(releases: list[dict], conn=None) -> None:
    """Insert newly discovered releases.

    Each dict must have keys: artist_id, id, name, album_type, release_date.
    """
    if not releases:
        return

    own_conn = conn is None
    conn = conn or _get_conn()
    try:
        with conn.cursor() as cur:
            for release in releases:
                cur.execute(
                    """
                    INSERT INTO spotify_radar.artist_releases
                        (artist_id, album_id, album_name, album_type, release_date)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (artist_id, album_id) DO NOTHING
                    """,
                    (
                        release["artist_id"],
                        release["id"],
                        release["name"],
                        release["album_type"],
                        release["release_date"],
                    ),
                )
        conn.commit()
    finally:
        if own_conn:
            conn.close()


def log_alert(
    dag_id: str,
    task_id: str,
    logical_date,
    error_message: str,
    log_url: str,
    conn=None,
) -> None:
    """Record a structured failure alert for audit history."""
    own_conn = conn is None
    conn = conn or _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO spotify_radar.alert_log
                    (dag_id, task_id, logical_date, error_message, log_url)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (dag_id, task_id, logical_date, error_message, log_url),
            )
        conn.commit()
    finally:
        if own_conn:
            conn.close()
