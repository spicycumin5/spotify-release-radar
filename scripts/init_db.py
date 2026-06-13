"""One-time setup script: applies include/sql/schema.sql to the local
Astro Postgres instance.

Run this once after `astro dev start` is up:

    python scripts/init_db.py

Connects using the default local Astro Postgres credentials
(postgres/postgres on localhost:5432). Override via env vars if needed.
"""

import os
from pathlib import Path

import psycopg2

SCHEMA_FILE = Path(__file__).resolve().parent.parent / "include" / "sql" / "schema.sql"


def main() -> None:
    conn = psycopg2.connect(
        host=os.environ.get("PGHOST", "localhost"),
        port=os.environ.get("PGPORT", "5432"),
        dbname=os.environ.get("PGDATABASE", "postgres"),
        user=os.environ.get("PGUSER", "postgres"),
        password=os.environ.get("PGPASSWORD", "postgres"),
    )
    try:
        with conn, conn.cursor() as cur:
            cur.execute(SCHEMA_FILE.read_text())
        print("Schema applied successfully.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
