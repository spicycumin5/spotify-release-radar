"""Postgres helpers for the read-only dashboard API.

Connects directly to the same local Postgres instance the Airflow pipeline
writes to (see include/db.py), using the same PG*/`.env` convention already
established in scripts/init_db.py -- this module runs outside Airflow, as a
standalone service, so it can't use PostgresHook.
"""

from __future__ import annotations

import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()


def _get_conn():
    return psycopg2.connect(
        host=os.environ.get("PGHOST", "localhost"),
        port=os.environ.get("PGPORT", "5432"),
        dbname=os.environ.get("PGDATABASE", "postgres"),
        user=os.environ.get("PGUSER", "postgres"),
        password=os.environ.get("PGPASSWORD", "postgres"),
    )


def list_releases(limit: int = 50, conn=None) -> list[dict]:
    """Return the most recently discovered releases, newest first."""
    own_conn = conn is None
    conn = conn or _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT r.artist_id, a.artist_name, r.album_id, r.album_name,
                       r.album_type, r.release_date, r.discovered_at
                FROM spotify_radar.artist_releases r
                JOIN spotify_radar.tracked_artists a ON a.artist_id = r.artist_id
                ORDER BY r.discovered_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            return [
                {
                    "artist_id": row[0],
                    "artist_name": row[1],
                    "album_id": row[2],
                    "album_name": row[3],
                    "album_type": row[4],
                    "release_date": row[5],
                    "discovered_at": row[6],
                }
                for row in cur.fetchall()
            ]
    finally:
        if own_conn:
            conn.close()


def list_artists(conn=None) -> list[dict]:
    """Return all tracked artists, alphabetical by name."""
    own_conn = conn is None
    conn = conn or _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT artist_id, artist_name, last_checked_at
                FROM spotify_radar.tracked_artists
                ORDER BY artist_name ASC
                """
            )
            return [
                {"artist_id": row[0], "artist_name": row[1], "last_checked_at": row[2]}
                for row in cur.fetchall()
            ]
    finally:
        if own_conn:
            conn.close()


def list_alerts(limit: int = 50, conn=None) -> list[dict]:
    """Return the most recent failure alerts, newest first."""
    own_conn = conn is None
    conn = conn or _get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, dag_id, task_id, logical_date, error_message,
                       log_url, alerted_at
                FROM spotify_radar.alert_log
                ORDER BY alerted_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            return [
                {
                    "id": row[0],
                    "dag_id": row[1],
                    "task_id": row[2],
                    "logical_date": row[3],
                    "error_message": row[4],
                    "log_url": row[5],
                    "alerted_at": row[6],
                }
                for row in cur.fetchall()
            ]
    finally:
        if own_conn:
            conn.close()
