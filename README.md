# Spotify Release Radar — an Airflow Pipeline + Alerting Framework

A small Apache Airflow project: it polls the artists its users
follow on Spotify for new releases, loads them into Postgres, and emails each
user a daily digest ordered by their listening history. On top of that ETL pipeline sits a **custom failure-alerting
framework** — a structured `on_failure_callback` that captures `dag_id`,
`task_id`, the run's logical date, and the error message, then both emails a
human and writes an audit row to a Postgres `alert_log` table.

## Architecture

```
                ┌──────────────────────────────────┐
   Spotify Web  │  spotify_release_radar (daily)    │   Postgres (Astro local)
   API          │                                   │   schema: spotify_radar
       │        │  list_user_batches                │◀── users
       │        │  sync_users.expand(batch)  ───────┼──▶ user_follows, user_artist_affinity
       │───────▶│  discover_releases (once) ────────┼──▶ releases, artist_releases
                │  deliver_digests.expand(batch) ───┼──▶ digests, digest_items
                │        └──────────────────────────┼──▶ Email (one digest per user per day)
                │                                   │
                │  [on_failure_callback]────────────┼──▶ alert_log table
                └──────────────────────────────────┘  └─▶ Email (structured failure alert)

   pipeline_failure_demo DAG (toggle an Airflow Variable to force a
   failure, used to demo/test the alerting framework end-to-end)
```

## How it scales and stays correct

- **Fan-out with shared work.** Users are split into batches and each batch
  is a dynamically mapped task. Follow lists are per user, but release
  checks run once per distinct artist (`discover_releases`) using one app
  token (client credentials) — 1,000 users following the same artist cost
  the same as one. Each check is two `/artists/{id}/albums` calls, one for
  albums and one for singles: a combined request lists albums first, so for
  a prolific artist new singles could fall off the page. Spotify has no
  batch endpoint for artist albums, so this per-artist cost is the floor.
- **Fewer redundant calls.** Artist checks send the previous `ETag` as
  `If-None-Match` (a 304 means nothing changed; if Spotify doesn't send an
  ETag, it's an ordinary request). Artists checked within
  `SPOTIFY_ARTIST_MIN_RECHECK_HOURS`, follow lists synced within
  `RADAR_FOLLOWS_MAX_AGE_HOURS`, and top artists synced within
  `RADAR_AFFINITY_MAX_AGE_HOURS` aren't re-fetched, so re-runs and manual
  triggers are close to free. Artists no active user follows are never checked.
- **Backoff and rate limits** (`include/spotify_client.py`). Requests are
  paced (`SPOTIFY_MIN_REQUEST_INTERVAL`); 5xx/timeouts/connection errors
  retry with capped exponential backoff and full jitter. A 429 with a short
  `Retry-After` is waited out (never retried early); a long one stops the
  run and records a cooldown in `spotify_radar.api_cooldown`, which later
  tasks and runs respect instead of retrying into a longer lockout.
- **Only genuinely new releases.** An artist's first check is a baseline
  that's never emailed; a user only gets releases discovered after they
  followed the artist; and a release dated more than
  `RADAR_MAX_RELEASE_AGE_DAYS` (default 30) before the run date is never
  emailed, which keeps reissues and back-catalog albums that surface late
  out of digests. They're still recorded and shown on the dashboard.
- **Dedup by album id.** `releases` has one row per Spotify album id;
  `artist_releases` links it to each artist it was found under. A
  collaboration between two artists you follow shows up once.
- **Idempotent delivery.** Each user has at most one digest per run date
  (`ds`), and an album can be in at most one digest per user
  (`digest_items` primary key). A digest is claimed (`pending → sending`)
  before the email goes out and marked `sent` after, so clearing or
  re-running a day never sends a second email. If the send raises, the
  claim is released so a re-run retries it; if the worker dies mid-send,
  the digest stays `sending` and is deliberately not resent (at-most-once).
- **Ranking** (`include/ranking.py`). Each user's top artists (short- and
  medium-term) become per-artist affinity scores; a release scores by its
  best-liked artist, plus small bonuses for full albums and fresh releases.

## Project structure

```
dags/
  spotify_release_radar.py   # main ETL DAG (TaskFlow API)
  pipeline_failure_demo.py    # tiny DAG to deliberately trigger the alerting framework
include/
  spotify_client.py           # Spotify Web API client (backoff, rate limits, ETags)
  radar.py                     # pipeline steps: sync users, discover, deliver digests
  ranking.py                   # orders digests by listening history
  db.py                        # Postgres helpers for the spotify_radar schema
  alerting.py                  # on_failure_callback: email + audit log row
  sql/schema.sql               # CREATE SCHEMA spotify_radar + tables
scripts/
  spotify_auth_setup.py        # OAuth script: register a user (or print a refresh token)
  init_db.py                    # applies schema.sql to local Postgres
  run_radar.py                  # runs the pipeline once without Airflow (used by GitHub Actions)
.github/workflows/
  release_radar.yml             # daily scheduled run on GitHub, no Docker needed
api/                             # read-only dashboard API (standalone FastAPI service)
  main.py                        # GET /releases, /artists, /alerts
  db.py                          # Postgres queries backing those routes
frontend/                        # Vite + React dashboard UI, consumes api/
signup/                          # signup + unsubscribe site (FastAPI on Vercel, self-contained)
  app.py
  requirements.txt
tests/
  test_dag_integrity.py         # DagBag import + sanity checks
  test_spotify_client.py        # backoff, 429s, ETags, diff/normalization (mocked HTTP)
  test_radar.py                  # fan-out, dedup, idempotent delivery (in-memory DB)
  test_ranking.py                # listening-history ranking
  test_db.py                     # SQL helpers (mocked DB)
  test_alerting.py               # on_failure_callback (mocked email/DB)
  test_api_db.py                 # dashboard API's query layer (mocked DB)
  test_run_radar.py              # Airflow-free runner: step order, failure alerts, SMTP
  test_signup.py                 # signup OAuth flow and unsubscribe pages (mocked Spotify/DB)
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

### 3. Register users

```bash
python -m venv .venv
.venv/Scripts/activate          # Windows
pip install -r requirements.txt

# Make sure SPOTIFY_CLIENT_ID / SPOTIFY_CLIENT_SECRET are exported, then:
python scripts/spotify_auth_setup.py
```

Follow the printed URL, authorize the app, and paste the redirect URL back in:

```bash
# Register a user (needs the schema from step 5 and Postgres running):
python scripts/spotify_auth_setup.py "<redirect URL>" --email them@example.com

# Or, for a single-user setup, print a token for .env instead:
python scripts/spotify_auth_setup.py "<redirect URL>"
```

With only `SPOTIFY_REFRESH_TOKEN` and `ALERT_EMAIL_TO` in `.env`, the DAG
registers that account as the first user on its next run. Repeat the
`--email` flow for each additional user; while the Spotify app is in
Development Mode, each user also has to be added under the app's **User
Management** settings. Tokens issued before this version lack the
`user-top-read` scope — those users still get digests, just not ordered by
listening history, until they re-authorize.

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
its tables. It's safe to re-run, and upgrades a database created by the
earlier single-user version in place.

### 6. Run the pipeline

In the Airflow UI, unpause and trigger `spotify_release_radar`. On success
you'll see `user_follows`, `tracked_artists`, and `releases` populate. The
first check of each artist records its current catalog as a baseline;
releases found after that land in each follower's next digest email.

## Scheduled runs without Docker (GitHub Actions)

Airflow only sends digests while `astro dev start` is running. To have them
go out every day regardless, `.github/workflows/release_radar.yml` runs
`scripts/run_radar.py` on GitHub's servers once a day (15:00 UTC; change the
`cron` line to move it). The runner calls the same `include/radar.py` steps
as the DAG, with the same failure handling: failed steps are written to
`alert_log` and emailed to `ALERT_EMAIL_TO`, and the run is marked failed.

Because the database also lives in Docker locally, this needs a hosted
Postgres that's always reachable. Any provider works; [Neon](https://neon.tech)'s
free tier is enough.

1. **Create the database.** Make a Neon project and copy its connection
   parameters (host, database, user, password).
2. **Point your `.env` at it** so local scripts and the dashboard use the
   same database:

   ```bash
   PGHOST=ep-xxxx.us-east-2.aws.neon.tech
   PGDATABASE=neondb
   PGUSER=neondb_owner
   PGPASSWORD=...
   PGSSLMODE=require
   ```

3. **Create the schema and register users** there:
   `python scripts/init_db.py`, then the `--email` flow from step 3 of
   Setup for each user. (Or skip registration and set the
   `SPOTIFY_REFRESH_TOKEN` secret below for a single user.) Data in the
   local Docker Postgres isn't copied over: the first run records each
   artist's catalog as a baseline, and digests start from the next new release.
4. **Add repository secrets** (GitHub → Settings → Secrets and variables →
   Actions): `SPOTIFY_CLIENT_ID`, `SPOTIFY_CLIENT_SECRET`, `ALERT_EMAIL_TO`,
   `SMTP_USER` (your Gmail address), `SMTP_PASSWORD` (the Gmail app
   password), `PGHOST`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`, and optionally
   `SPOTIFY_REFRESH_TOKEN`.
5. **Push to GitHub and test it:** Actions → *Release radar* → *Run
   workflow*. You can also run it locally with `python scripts/run_radar.py`.

Notes:

- Scheduled runs can start several minutes late when GitHub is busy.
- In a public repo, GitHub turns scheduled workflows off after 60 days with
  no commits; re-enable it from the Actions tab if that happens.
- Don't run the Airflow DAG against a different database with the same
  users, or they'll get two digests. Either pause the DAG or point it at the
  hosted database too (`AIRFLOW_CONN_POSTGRES_DEFAULT` in `.env`). On a
  shared database, an album never appears in two digests for the same user.

## Inviting friends: signup and unsubscribe

`signup/app.py` is a small FastAPI site deployed on Vercel
(https://release-radar-signup.vercel.app). A friend enters the email they want
digests sent to, approves the app on Spotify, and is registered in
`spotify_radar.users`. No URLs to copy back and forth.

Every digest links to `/unsubscribe/<token>` (a random per-user token in
`users.unsubscribe_token`) and carries `List-Unsubscribe` headers, so Gmail
shows its own "Unsubscribe" button. Opening the link only shows a confirmation
page, because mail scanners open links in emails; the button (or Gmail's
one-click POST) sets the user inactive. Signing up again reactivates them.

**Inviting someone:** Spotify Development Mode apps are capped at 5 users, and
only accounts listed under the app's **User Management** in the
[Spotify dashboard](https://developer.spotify.com/dashboard) can sign in, so
that list is the guest list. Add their Spotify account email there, then send
them the signup link. Anyone not on the list gets a page saying to ask for an
invite.

**Deploying changes:** the Vercel project deploys only the `signup/` folder,
which is why the site doesn't import `include/` (the project requirements
pull in Airflow). The SQL in `register_user` mirrors `include/db.py`'s
`upsert_user`.

```bash
npx vercel deploy --prod --cwd signup
```

Its environment variables (set in the Vercel project) are
`SPOTIFY_CLIENT_ID`, `SPOTIFY_CLIENT_SECRET`, the `PG*` settings for the
hosted database, and `RADAR_PUBLIC_URL`. The Spotify app needs
`<RADAR_PUBLIC_URL>/callback` as a redirect URI. The daily job reads
`RADAR_PUBLIC_URL` from a GitHub repository variable to build unsubscribe
links; without it, digests are sent without one.

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

## Dashboard (API + frontend)

A small read-only dashboard sits on top of the same `spotify_radar` Postgres
tables the pipeline writes to — a FastAPI service (`api/`) and a Vite + React
frontend (`frontend/`). Both are standalone from the Airflow/Astro image; they
connect to the same local Postgres the pipeline uses (`localhost:5432`), the
same way `scripts/init_db.py` does.

Requires the local Postgres to be up (`astro dev start`) and populated (the
DAG has run at least once — otherwise the panels just show their empty state).

```bash
# API (from repo root)
pip install -r api/requirements.txt
uvicorn api.main:app --reload --port 8000

# Frontend (separate terminal)
cd frontend
npm install
npm run dev   # http://localhost:5173
```

`GET /releases`, `GET /artists`, and `GET /alerts` are the three endpoints;
the frontend polls them on load and on manual refresh. No auth, no write
endpoints — this is a local viewer, not a public API.

## Testing

```bash
pytest
```

All tests are fully mocked, with no live Spotify, SMTP, or Postgres calls. They
cover: DAG import/integrity (`test_dag_integrity.py`), the Spotify client's
backoff/rate-limit/ETag handling and diff logic (`test_spotify_client.py`),
the pipeline's fan-out, dedup, and never-double-send guarantees against an
in-memory DB (`test_radar.py`), digest ranking (`test_ranking.py`), the SQL
helpers (`test_db.py`), the alerting
callback's email/audit-log content (`test_alerting.py`), the dashboard
API's query layer (`test_api_db.py`), the Airflow-free scheduled runner
(`test_run_radar.py`), and the signup/unsubscribe site (`test_signup.py`).

## Future work

- **Auth on the dashboard API**: JWT-based authentication and per-API-key
  usage tracking, if this ever needed to be exposed beyond localhost —
  directly mapping to Astro Core Services' other focus areas (Public API,
  auth, billing).
- A Slack webhook as an additional/alternative alert channel alongside email.
- A GitHub Actions workflow running `pytest` and DAG validation on every push.
