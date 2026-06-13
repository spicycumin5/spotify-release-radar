# Spotify Release Radar — an Airflow Pipeline + Alerting Framework

A small but real Apache Airflow project: it polls the artists you follow on
Spotify for new releases, loads them into Postgres, and emails you a
summary. On top of that ETL pipeline sits a **custom failure-alerting
framework** — a structured `on_failure_callback` that captures `dag_id`,
`task_id`, the run's logical date, and the error message, then both emails a
human and writes an audit row to a Postgres `alert_log` table.

This project was built to demonstrate the kind of work Astronomer's **Astro
Core Services** team does: real orchestration logic plus observability
("alerting frameworks") on top of it — not just "write a DAG."

## Architecture

```
                ┌─────────────────────────┐
   Spotify Web  │  spotify_release_radar   │   Postgres (Astro local)
   API (OAuth,  │  DAG (daily / manual)    │   schema: spotify_radar
   refresh tok) │                          │
       │        │  sync_followed_artists   │──▶ tracked_artists
       │───────▶│  fetch_and_diff_releases │──▶ artist_releases
                │  load_new_releases       │
                │  notify_new_releases ────┼──▶ Email (new releases found)
                │                          │
                │  [on_failure_callback]───┼──▶ alert_log table
                └─────────────────────────┘  └─▶ Email (structured failure alert)

   pipeline_failure_demo DAG (toggle an Airflow Variable to force a
   failure, used to demo/test the alerting framework end-to-end)
```

## Project structure

```
dags/
  spotify_release_radar.py   # main ETL DAG (TaskFlow API)
  pipeline_failure_demo.py    # tiny DAG to deliberately trigger the alerting framework
include/
  spotify_client.py           # Spotify Web API client (refresh-token based)
  db.py                        # Postgres helpers for the spotify_radar schema
  alerting.py                  # on_failure_callback: email + audit log row
  sql/schema.sql               # CREATE SCHEMA spotify_radar + tables
scripts/
  spotify_auth_setup.py        # one-time OAuth script -> refresh token
  init_db.py                    # applies schema.sql to local Postgres
tests/
  test_dag_integrity.py         # DagBag import + sanity checks
  test_spotify_client.py        # diff/normalization logic (mocked HTTP)
  test_alerting.py               # on_failure_callback (mocked email/DB)
```

## Setup

### 1. Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (running)
- [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli) —
  on Windows: `winget install -e --id Astronomer.Astro`
- A Spotify account with at least one followed artist
- A Gmail account with an
  [app password](https://myaccount.google.com/apppasswords) for sending
  alert emails

### 2. Configure environment variables

```bash
cp .env.example .env
```

Fill in `SPOTIFY_CLIENT_ID` / `SPOTIFY_CLIENT_SECRET` from a new app at the
[Spotify Developer Dashboard](https://developer.spotify.com/dashboard)
(redirect URI: `http://127.0.0.1:8888/callback`), and the `AIRFLOW__SMTP__*`
/ `ALERT_EMAIL_TO` values for your Gmail account.

### 3. Get a Spotify refresh token (one-time)

```bash
python -m venv .venv
.venv/Scripts/activate          # Windows
pip install -r requirements.txt

# Make sure SPOTIFY_CLIENT_ID / SPOTIFY_CLIENT_SECRET are exported, then:
python scripts/spotify_auth_setup.py
```

Follow the printed URL, authorize the app, paste the redirect URL back in,
and copy the resulting `SPOTIFY_REFRESH_TOKEN` into `.env`.

### 4. Start Airflow locally

```bash
astro dev start
```

This builds the project image, starts Airflow (UI at
[localhost:8080](http://localhost:8080), `admin`/`admin`), and starts a local
Postgres instance (mapped to `localhost:5432`, `postgres`/`postgres`). The
`postgres_default` Airflow connection and `FORCE_FAILURE` variable are
pre-configured via `airflow_settings.yaml`.

### 5. Create the application schema

```bash
python scripts/init_db.py
```

This connects to the local Postgres (from the host, via `localhost:5432`)
and applies `include/sql/schema.sql`, creating the `spotify_radar` schema and
its three tables.

### 6. Run the pipeline

In the Airflow UI, unpause and trigger `spotify_release_radar`. On success
you'll see `tracked_artists` and `artist_releases` populate, and — if any new
releases were found — a summary email.

## The alerting framework

`include/alerting.py` defines `on_failure_callback(context)`, wired into
**every task in every DAG** via `default_args`. When a task fails, it:

1. Pulls structured context from the Airflow `context` dict: `dag_id`,
   `task_id`, `logical_date`, the exception message, and a direct link to the
   task's logs.
2. Writes an audit row to `spotify_radar.alert_log`.
3. Sends an HTML email with that same context, so a human is notified without
   needing to check the Airflow UI.

### Demo it end-to-end

1. In the Airflow UI, go to **Admin → Variables** and set `FORCE_FAILURE` to
   `true`.
2. Trigger `pipeline_failure_demo`. Its single task raises on purpose.
3. Check your inbox for a "🚨 Airflow failure: pipeline_failure_demo.maybe_fail"
   email, and query `spotify_radar.alert_log` to see the audit row.
4. Set `FORCE_FAILURE` back to `false`.

## Testing

```bash
pytest
```

All tests are fully mocked — no live Spotify, SMTP, or Postgres calls. They
cover: DAG import/integrity (`test_dag_integrity.py`), the new-release diff
and date-normalization logic (`test_spotify_client.py`), and the alerting
callback's email/audit-log content (`test_alerting.py`).

## Future work

- **Public API layer**: a small FastAPI service exposing read-only endpoints
  (`GET /releases`, `GET /alerts`) over the same Postgres tables, with
  JWT-based authentication and per-API-key usage tracking — directly mapping
  to Astro Core Services' other focus areas (Public API, auth, billing).
- A Slack webhook as an additional/alternative alert channel alongside email.
- A GitHub Actions workflow running `pytest` and DAG validation on every push.
