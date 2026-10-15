"""Postgres helpers for the spotify_radar schema (see include/sql/schema.sql).

Every function accepts an optional DB-API `conn` so it can be unit tested
with a mock connection. When omitted, a connection is pulled from Airflow's
`postgres_default` connection via PostgresHook, unless a connection factory
was registered with use_connection_factory() (scripts/run_radar.py does this
to run the pipeline outside Airflow).
"""

from __future__ import annotations

from contextlib import contextmanager
from datetime import date


_connection_factory = None


def use_connection_factory(factory) -> None:
    """Open connections with `factory()` instead of Airflow's PostgresHook."""
    global _connection_factory
    _connection_factory = factory


def _get_conn():
    if _connection_factory is not None:
        return _connection_factory()

    from airflow.providers.postgres.hooks.postgres import PostgresHook

    return PostgresHook(postgres_conn_id="postgres_default").get_conn()


@contextmanager
def _connection(conn=None):
    """Yield `conn`, or open (and afterwards close) one if it's None."""
    own_conn = conn is None
    conn = conn or _get_conn()
    try:
        yield conn
    finally:
        if own_conn:
            conn.close()


# --- Users -------------------------------------------------------------------


def upsert_user(user_id: str, display_name: str | None, email: str, refresh_token: str, conn=None) -> None:
    """Register a user, or update an existing user's details and token."""
    with _connection(conn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO spotify_radar.users (user_id, display_name, email, refresh_token)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (user_id) DO UPDATE
                    SET display_name = EXCLUDED.display_name,
                        email = EXCLUDED.email,
                        refresh_token = EXCLUDED.refresh_token,
                        active = TRUE
                """,
                (user_id, display_name, email, refresh_token),
            )
        conn.commit()


def count_users(conn=None) -> int:
    with _connection(conn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM spotify_radar.users")
            return cur.fetchone()[0]


def get_active_user_ids(conn=None) -> list[str]:
    with _connection(conn) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id FROM spotify_radar.users WHERE active ORDER BY user_id")
            return [row[0] for row in cur.fetchall()]


def get_users(user_ids: list[str], conn=None) -> list[dict]:
    """Return the active users among `user_ids`, including their refresh tokens."""
    with _connection(conn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT user_id, display_name, email, refresh_token,
                       follows_synced_at, affinity_synced_at
                FROM spotify_radar.users
                WHERE user_id = ANY(%s) AND active
                ORDER BY user_id
                """,
                (user_ids,),
            )
            return [
                {
                    "user_id": row[0],
                    "display_name": row[1],
                    "email": row[2],
                    "refresh_token": row[3],
                    "follows_synced_at": row[4],
                    "affinity_synced_at": row[5],
                }
                for row in cur.fetchall()
            ]


def update_refresh_token(user_id: str, refresh_token: str, conn=None) -> None:
    with _connection(conn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE spotify_radar.users SET refresh_token = %s WHERE user_id = %s",
                (refresh_token, user_id),
            )
        conn.commit()


def replace_user_follows(user_id: str, artists: list[dict], conn=None) -> None:
    """Make `artists` the user's complete follow list.

    New artists join tracked_artists with last_checked_at = NULL, so
    get_artists_to_check() picks them up first. Existing follows keep their
    original followed_since; unfollowed artists are removed.
    """
    with _connection(conn) as conn:
        with conn.cursor() as cur:
            for artist in artists:
                cur.execute(
                    """
                    INSERT INTO spotify_radar.tracked_artists (artist_id, artist_name, last_checked_at)
                    VALUES (%s, %s, NULL)
                    ON CONFLICT (artist_id) DO UPDATE
                        SET artist_name = EXCLUDED.artist_name
                    """,
                    (artist["id"], artist["name"]),
                )
                cur.execute(
                    """
                    INSERT INTO spotify_radar.user_follows (user_id, artist_id)
                    VALUES (%s, %s)
                    ON CONFLICT (user_id, artist_id) DO NOTHING
                    """,
                    (user_id, artist["id"]),
                )
            cur.execute(
                "DELETE FROM spotify_radar.user_follows WHERE user_id = %s AND NOT (artist_id = ANY(%s))",
                (user_id, [artist["id"] for artist in artists]),
            )
            cur.execute(
                "UPDATE spotify_radar.users SET follows_synced_at = now() WHERE user_id = %s",
                (user_id,),
            )
        conn.commit()


def replace_user_affinity(user_id: str, scores: dict[str, float], conn=None) -> None:
    """Replace the user's artist affinity scores (see include/ranking.py)."""
    with _connection(conn) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM spotify_radar.user_artist_affinity WHERE user_id = %s", (user_id,))
            for artist_id, score in scores.items():
                cur.execute(
                    """
                    INSERT INTO spotify_radar.user_artist_affinity (user_id, artist_id, score)
                    VALUES (%s, %s, %s)
                    """,
                    (user_id, artist_id, score),
                )
            cur.execute(
                "UPDATE spotify_radar.users SET affinity_synced_at = now() WHERE user_id = %s",
                (user_id,),
            )
        conn.commit()


def get_user_affinity(user_id: str, conn=None) -> dict[str, float]:
    with _connection(conn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT artist_id, score FROM spotify_radar.user_artist_affinity WHERE user_id = %s",
                (user_id,),
            )
            return {row[0]: row[1] for row in cur.fetchall()}


# --- Artist release checks ---------------------------------------------------


def get_artists_to_check(limit: int, min_recheck_hours: float, conn=None) -> list[dict]:
    """Return up to `limit` artists due for a release check.

    Only artists followed by at least one active user are checked, each once
    no matter how many users follow it. Artists checked within the last
    `min_recheck_hours` are skipped, so re-running a day doesn't re-spend
    API calls. Ordered by last_checked_at ascending (NULLs first), so
    newly-followed and longest-overdue artists go first.
    """
    with _connection(conn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT a.artist_id, a.artist_name, a.albums_etag, a.singles_etag
                FROM spotify_radar.tracked_artists a
                WHERE EXISTS (
                        SELECT 1
                        FROM spotify_radar.user_follows f
                        JOIN spotify_radar.users u ON u.user_id = f.user_id
                        WHERE f.artist_id = a.artist_id AND u.active
                    )
                  AND (a.last_checked_at IS NULL
                       OR a.last_checked_at < now() - make_interval(secs => %s))
                ORDER BY a.last_checked_at ASC NULLS FIRST
                LIMIT %s
                """,
                (min_recheck_hours * 3600, limit),
            )
            return [
                {"id": row[0], "name": row[1], "etags": {"album": row[2], "single": row[3]}}
                for row in cur.fetchall()
            ]


def get_seen_release_ids(artist_id: str, conn=None) -> set[str]:
    """Return album_ids already recorded for this artist."""
    with _connection(conn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT album_id FROM spotify_radar.artist_releases WHERE artist_id = %s",
                (artist_id,),
            )
            return {row[0] for row in cur.fetchall()}


def record_artist_check(
    artist_id: str,
    releases: list[dict],
    is_baseline: bool,
    etags: dict[str, str | None],
    conn=None,
) -> None:
    """Store an artist's newly found releases and mark the artist checked.

    Releases are deduplicated by Spotify album id: an album already in the
    catalog (e.g. a collaboration found under another artist) isn't
    re-inserted, only linked to this artist too. Everything commits in one
    transaction, so a crash can't record releases without marking the check.

    Each release dict needs: id, name, album_type, release_date.
    """
    with _connection(conn) as conn:
        with conn.cursor() as cur:
            for release in releases:
                values = (release["id"], release["name"], release["album_type"], release["release_date"])
                cur.execute(
                    """
                    INSERT INTO spotify_radar.releases (album_id, album_name, album_type, release_date)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (album_id) DO NOTHING
                    """,
                    values,
                )
                cur.execute(
                    """
                    INSERT INTO spotify_radar.artist_releases
                        (artist_id, album_id, album_name, album_type, release_date, is_baseline)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (artist_id, album_id) DO NOTHING
                    """,
                    (artist_id, *values, is_baseline),
                )
            cur.execute(
                """
                UPDATE spotify_radar.tracked_artists
                SET last_checked_at = now(), albums_etag = %s, singles_etag = %s
                WHERE artist_id = %s
                """,
                (etags.get("album"), etags.get("single"), artist_id),
            )
        conn.commit()


# --- Rate-limit circuit breaker ----------------------------------------------


def get_cooldown_remaining(api: str = "spotify", conn=None) -> float:
    """Seconds until `api` may be called again (0 if not cooling down)."""
    with _connection(conn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT EXTRACT(EPOCH FROM cooldown_until - now())
                FROM spotify_radar.api_cooldown
                WHERE api = %s AND cooldown_until > now()
                """,
                (api,),
            )
            row = cur.fetchone()
            return float(row[0]) if row else 0.0


def set_cooldown(seconds: float, api: str = "spotify", conn=None) -> None:
    """Block calls to `api` for `seconds` (never shortening an existing cooldown)."""
    with _connection(conn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO spotify_radar.api_cooldown (api, cooldown_until)
                VALUES (%s, now() + make_interval(secs => %s))
                ON CONFLICT (api) DO UPDATE
                    SET cooldown_until = GREATEST(spotify_radar.api_cooldown.cooldown_until,
                                                  EXCLUDED.cooldown_until)
                """,
                (api, seconds),
            )
        conn.commit()


# --- Digests -----------------------------------------------------------------


def get_pending_releases(user_id: str, released_since: date, conn=None) -> list[dict]:
    """Releases the user should hear about that aren't in any digest yet.

    A release qualifies if it was found under an artist the user follows,
    wasn't part of that artist's baseline, was discovered after the user
    started following, came out on or after `released_since`, and hasn't
    been put in one of the user's digests. The release-date cutoff keeps
    old catalog items out: reissues with new album ids, or older albums
    that only now show up in an artist's listing.
    Grouped by album id, so a collaboration between two followed artists
    appears once, credited to both.
    """
    with _connection(conn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT r.album_id, r.album_name, r.album_type, r.release_date,
                       array_agg(DISTINCT ar.artist_id), array_agg(DISTINCT a.artist_name)
                FROM spotify_radar.user_follows f
                JOIN spotify_radar.artist_releases ar
                  ON ar.artist_id = f.artist_id
                 AND NOT ar.is_baseline
                 AND ar.discovered_at >= f.followed_since
                JOIN spotify_radar.releases r ON r.album_id = ar.album_id
                JOIN spotify_radar.tracked_artists a ON a.artist_id = ar.artist_id
                WHERE f.user_id = %s
                  AND r.release_date >= %s
                  AND NOT EXISTS (
                      SELECT 1 FROM spotify_radar.digest_items di
                      WHERE di.user_id = f.user_id AND di.album_id = r.album_id
                  )
                GROUP BY r.album_id, r.album_name, r.album_type, r.release_date
                """,
                (user_id, released_since),
            )
            return [
                {
                    "album_id": row[0],
                    "album_name": row[1],
                    "album_type": row[2],
                    "release_date": row[3],
                    "artist_ids": list(row[4]),
                    "artist_names": sorted(row[5]),
                }
                for row in cur.fetchall()
            ]


def add_digest_items(user_id: str, digest_date: date, items: list[dict], conn=None) -> int:
    """Put ranked releases into the user's digest for `digest_date`.

    Creates the digest if needed. Does nothing if that digest is already
    being sent or was sent -- those releases stay pending for the next run's
    digest. Returns how many releases were added.
    """
    if not items:
        return 0

    with _connection(conn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO spotify_radar.digests (user_id, digest_date)
                VALUES (%s, %s)
                ON CONFLICT (user_id, digest_date) DO NOTHING
                """,
                (user_id, digest_date),
            )
            cur.execute(
                """
                SELECT status FROM spotify_radar.digests
                WHERE user_id = %s AND digest_date = %s
                FOR UPDATE
                """,
                (user_id, digest_date),
            )
            if cur.fetchone()[0] != "pending":
                conn.rollback()
                return 0

            added = 0
            for item in items:
                cur.execute(
                    """
                    INSERT INTO spotify_radar.digest_items (user_id, album_id, digest_date, score)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (user_id, album_id) DO NOTHING
                    """,
                    (user_id, item["album_id"], digest_date, item["score"]),
                )
                added += cur.rowcount
        conn.commit()
        return added


def get_pending_digest_dates(user_id: str, up_to: date, conn=None) -> list[date]:
    """Dates of the user's unsent digests on or before `up_to`, oldest first."""
    with _connection(conn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT digest_date FROM spotify_radar.digests
                WHERE user_id = %s AND status = 'pending' AND digest_date <= %s
                ORDER BY digest_date
                """,
                (user_id, up_to),
            )
            return [row[0] for row in cur.fetchall()]


def claim_digest(user_id: str, digest_date: date, conn=None) -> bool:
    """Atomically move a digest from pending to sending.

    Returns False if it was already claimed or sent -- the caller must not
    send it then. This is what makes delivery at-most-once.
    """
    with _connection(conn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE spotify_radar.digests SET status = 'sending'
                WHERE user_id = %s AND digest_date = %s AND status = 'pending'
                """,
                (user_id, digest_date),
            )
            claimed = cur.rowcount == 1
        conn.commit()
        return claimed


def mark_digest_sent(user_id: str, digest_date: date, conn=None) -> None:
    with _connection(conn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE spotify_radar.digests SET status = 'sent', sent_at = now()
                WHERE user_id = %s AND digest_date = %s
                """,
                (user_id, digest_date),
            )
        conn.commit()


def release_digest_claim(user_id: str, digest_date: date, conn=None) -> None:
    """Return a claimed digest to pending after a send that definitely failed."""
    with _connection(conn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE spotify_radar.digests SET status = 'pending'
                WHERE user_id = %s AND digest_date = %s AND status = 'sending'
                """,
                (user_id, digest_date),
            )
        conn.commit()


def get_digest_items(user_id: str, digest_date: date, conn=None) -> list[dict]:
    """A digest's releases, highest-ranked first."""
    with _connection(conn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT r.album_id, r.album_name, r.album_type, r.release_date,
                       array_agg(DISTINCT a.artist_name), di.score
                FROM spotify_radar.digest_items di
                JOIN spotify_radar.releases r ON r.album_id = di.album_id
                JOIN spotify_radar.artist_releases ar ON ar.album_id = di.album_id
                JOIN spotify_radar.tracked_artists a ON a.artist_id = ar.artist_id
                WHERE di.user_id = %s AND di.digest_date = %s
                GROUP BY r.album_id, r.album_name, r.album_type, r.release_date, di.score
                ORDER BY di.score DESC, r.album_name
                """,
                (user_id, digest_date),
            )
            return [
                {
                    "album_id": row[0],
                    "album_name": row[1],
                    "album_type": row[2],
                    "release_date": row[3],
                    "artist_names": sorted(row[4]),
                    "score": row[5],
                }
                for row in cur.fetchall()
            ]


# --- Alerting ----------------------------------------------------------------


def log_alert(
    dag_id: str,
    task_id: str,
    logical_date,
    error_message: str,
    log_url: str,
    conn=None,
) -> None:
    """Record a structured failure alert for audit history."""
    with _connection(conn) as conn:
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
