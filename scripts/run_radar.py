"""Run the release radar once, without Airflow.

Used by .github/workflows/release_radar.yml so digests go out on a schedule
without Docker or Airflow running. Runs the same steps as
dags/spotify_release_radar.py, in the same order and with the same
failure semantics:

    list users -> sync_users -> discover_releases -> deliver_digests

A failed sync doesn't stop discovery, and a failed discovery doesn't stop
delivery. Each failure is recorded in spotify_radar.alert_log and emailed
to ALERT_EMAIL_TO, like include/alerting.py does for Airflow tasks, and the
script exits non-zero so the GitHub Actions run shows as failed.

Configuration is the same .env / environment variables the DAG uses:
SPOTIFY_*, ALERT_EMAIL_TO, AIRFLOW__SMTP__* for mail, and PG* (plus
PGSSLMODE for a hosted database) for Postgres.

    python scripts/run_radar.py                     # digest for today (UTC)
    python scripts/run_radar.py --date 2026-10-15   # a specific digest date

Delivery is idempotent per (user, digest date), so re-running a date never
sends anyone a second email.
"""

from __future__ import annotations

import argparse
import html
import logging
import os
import smtplib
import sys
from datetime import date, datetime, timezone
from email.message import EmailMessage
from pathlib import Path

import psycopg2
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from include import db, radar  # noqa: E402

log = logging.getLogger("run_radar")

DAG_ID = "spotify_release_radar"


def _env_bool(name: str, default: bool) -> bool:
    return os.environ.get(name, str(default)).strip().lower() in ("1", "true", "yes")


def pg_connect():
    # libpq also reads PGSSLMODE etc. from the environment.
    return psycopg2.connect(
        host=os.environ.get("PGHOST", "localhost"),
        port=os.environ.get("PGPORT", "5432"),
        dbname=os.environ.get("PGDATABASE", "postgres"),
        user=os.environ.get("PGUSER", "postgres"),
        password=os.environ.get("PGPASSWORD", "postgres"),
    )


def send_email(to: str, subject: str, html_content: str, custom_headers: dict | None = None) -> None:
    """Send an HTML email using the AIRFLOW__SMTP__* settings from .env."""
    msg = EmailMessage()
    msg["From"] = os.environ["AIRFLOW__SMTP__SMTP_MAIL_FROM"]
    msg["To"] = to
    msg["Subject"] = subject
    for name, value in (custom_headers or {}).items():
        msg[name] = value
    msg.set_content("This email has an HTML body; view it in an HTML-capable mail client.")
    msg.add_alternative(html_content, subtype="html")

    host = os.environ.get("AIRFLOW__SMTP__SMTP_HOST", "smtp.gmail.com")
    port = int(os.environ.get("AIRFLOW__SMTP__SMTP_PORT", "587"))
    use_ssl = _env_bool("AIRFLOW__SMTP__SMTP_SSL", False)
    smtp_cls = smtplib.SMTP_SSL if use_ssl else smtplib.SMTP
    with smtp_cls(host, port, timeout=30) as smtp:
        if not use_ssl and _env_bool("AIRFLOW__SMTP__SMTP_STARTTLS", True):
            smtp.starttls()
        user = os.environ.get("AIRFLOW__SMTP__SMTP_USER")
        if user:
            smtp.login(user, os.environ["AIRFLOW__SMTP__SMTP_PASSWORD"])
        smtp.send_message(msg)


def _run_url() -> str:
    """Link to this GitHub Actions run's logs, if running there."""
    if os.environ.get("GITHUB_RUN_ID"):
        return (
            f"{os.environ.get('GITHUB_SERVER_URL', 'https://github.com')}/"
            f"{os.environ['GITHUB_REPOSITORY']}/actions/runs/{os.environ['GITHUB_RUN_ID']}"
        )
    return ""


def alert(step: str, digest_date: date, error: Exception, send) -> None:
    """Record and email a step failure. Never raises: alerting must not mask the original error."""
    log_url = _run_url()
    try:
        db.log_alert(dag_id=DAG_ID, task_id=step, logical_date=digest_date, error_message=str(error), log_url=log_url)
    except Exception:
        log.exception("Failed to record alert for %s", step)

    log_link = f'<a href="{html.escape(log_url)}">{html.escape(log_url)}</a>' if log_url else "(local run)"
    html_content = f"""
    <h2>Release radar step failed</h2>
    <table border="1" cellpadding="6">
        <tr><th>Pipeline</th><td>{DAG_ID} (scheduled runner)</td></tr>
        <tr><th>Step</th><td>{html.escape(step)}</td></tr>
        <tr><th>Digest date</th><td>{digest_date}</td></tr>
        <tr><th>Error</th><td>{html.escape(str(error))}</td></tr>
        <tr><th>Logs</th><td>{log_link}</td></tr>
    </table>
    """
    try:
        send(
            to=os.environ["ALERT_EMAIL_TO"],
            subject=f"\U0001F6A8 Release radar failure: {step}",
            html_content=html_content,
        )
    except Exception:
        log.exception("Failed to send alert email for %s", step)


def run(digest_date: date, send=send_email) -> bool:
    """Run every step once. Returns True if all of them succeeded."""
    # Same env vars and defaults as dags/spotify_release_radar.py.
    artist_batch_size = int(os.environ.get("SPOTIFY_ARTIST_BATCH_SIZE", "20"))
    artist_min_recheck_hours = float(os.environ.get("SPOTIFY_ARTIST_MIN_RECHECK_HOURS", "20"))
    follows_max_age_hours = float(os.environ.get("RADAR_FOLLOWS_MAX_AGE_HOURS", "20"))
    affinity_max_age_hours = float(os.environ.get("RADAR_AFFINITY_MAX_AGE_HOURS", "72"))
    max_release_age_days = int(os.environ.get("RADAR_MAX_RELEASE_AGE_DAYS", "30"))

    failed = []

    def step(name, fn):
        log.info("Running %s", name)
        try:
            return fn()
        except Exception as e:
            log.exception("%s failed", name)
            failed.append(name)
            alert(name, digest_date, e, send)
            return None

    def list_users():
        radar.bootstrap_env_user()
        return db.get_active_user_ids()

    user_ids = step("list_users", list_users)
    if user_ids is None:
        return False

    step("sync_users", lambda: radar.sync_users(user_ids, follows_max_age_hours, affinity_max_age_hours))
    step("discover_releases", lambda: radar.discover_releases(artist_batch_size, artist_min_recheck_hours))
    step("deliver_digests", lambda: radar.deliver_digests(user_ids, digest_date, send, max_release_age_days))

    if failed:
        log.error("Finished with failures in: %s", ", ".join(failed))
    return not failed


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--date", type=date.fromisoformat, help="digest date (YYYY-MM-DD); default today in UTC")
    args = parser.parse_args()

    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    db.use_connection_factory(pg_connect)

    digest_date = args.date or datetime.now(timezone.utc).date()
    return 0 if run(digest_date) else 1


if __name__ == "__main__":
    sys.exit(main())
