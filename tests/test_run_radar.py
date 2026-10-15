"""Tests for scripts/run_radar.py, the Airflow-free scheduled runner (mocked radar/db/SMTP)."""

from __future__ import annotations

from datetime import date
from unittest import mock

import pytest

from include import db
from scripts import run_radar

DAY = date(2026, 10, 15)


@pytest.fixture
def pipeline(monkeypatch):
    """Mock every pipeline step and the alert log; returns the call-order list."""
    calls = []
    monkeypatch.setenv("ALERT_EMAIL_TO", "alerts@example.com")
    monkeypatch.setattr(run_radar.radar, "bootstrap_env_user", lambda: calls.append("bootstrap"))
    monkeypatch.setattr(run_radar.db, "get_active_user_ids", lambda: calls.append("list") or ["u1", "u2"])
    monkeypatch.setattr(run_radar.radar, "sync_users", lambda ids, *a: calls.append(("sync", ids)))
    monkeypatch.setattr(run_radar.radar, "discover_releases", lambda *a: calls.append("discover"))
    monkeypatch.setattr(
        run_radar.radar, "deliver_digests", lambda ids, d, send, *a: calls.append(("deliver", ids, d, send))
    )
    monkeypatch.setattr(run_radar.db, "log_alert", mock.Mock())
    return calls


def _raise(exc):
    def fn(*args, **kwargs):
        raise exc

    return fn


def test_runs_all_steps_in_order(pipeline):
    send = mock.Mock()

    assert run_radar.run(DAY, send=send) is True

    assert pipeline == ["bootstrap", "list", ("sync", ["u1", "u2"]), "discover", ("deliver", ["u1", "u2"], DAY, send)]
    send.assert_not_called()
    run_radar.db.log_alert.assert_not_called()


def test_failed_sync_still_discovers_and_delivers_and_alerts(pipeline, monkeypatch):
    monkeypatch.setattr(run_radar.radar, "sync_users", _raise(RuntimeError("token revoked")))
    send = mock.Mock()

    assert run_radar.run(DAY, send=send) is False

    assert "discover" in pipeline
    assert any(c[0] == "deliver" for c in pipeline if isinstance(c, tuple))
    run_radar.db.log_alert.assert_called_once()
    assert run_radar.db.log_alert.call_args.kwargs["task_id"] == "sync_users"
    assert run_radar.db.log_alert.call_args.kwargs["error_message"] == "token revoked"
    send.assert_called_once()
    assert send.call_args.kwargs["to"] == "alerts@example.com"
    assert "sync_users" in send.call_args.kwargs["subject"]


def test_failed_user_listing_stops_the_run(pipeline, monkeypatch):
    monkeypatch.setattr(run_radar.db, "get_active_user_ids", _raise(RuntimeError("db down")))

    assert run_radar.run(DAY, send=mock.Mock()) is False

    assert pipeline == ["bootstrap"]


def test_alerting_failure_does_not_raise(pipeline, monkeypatch):
    monkeypatch.setattr(run_radar.radar, "discover_releases", _raise(RuntimeError("boom")))
    run_radar.db.log_alert.side_effect = RuntimeError("db down")
    send = mock.Mock(side_effect=OSError("smtp down"))

    assert run_radar.run(DAY, send=send) is False
    assert any(c[0] == "deliver" for c in pipeline if isinstance(c, tuple))


def test_alert_links_github_run(pipeline, monkeypatch):
    monkeypatch.setenv("GITHUB_SERVER_URL", "https://github.com")
    monkeypatch.setenv("GITHUB_REPOSITORY", "me/radar")
    monkeypatch.setenv("GITHUB_RUN_ID", "42")

    run_radar.alert("discover_releases", DAY, RuntimeError("<x>"), mock.Mock())

    assert run_radar.db.log_alert.call_args.kwargs["log_url"] == "https://github.com/me/radar/actions/runs/42"


def test_send_email_uses_starttls_and_login(monkeypatch):
    monkeypatch.setenv("AIRFLOW__SMTP__SMTP_MAIL_FROM", "me@gmail.com")
    monkeypatch.setenv("AIRFLOW__SMTP__SMTP_HOST", "smtp.gmail.com")
    monkeypatch.setenv("AIRFLOW__SMTP__SMTP_PORT", "587")
    monkeypatch.setenv("AIRFLOW__SMTP__SMTP_STARTTLS", "True")
    monkeypatch.setenv("AIRFLOW__SMTP__SMTP_SSL", "False")
    monkeypatch.setenv("AIRFLOW__SMTP__SMTP_USER", "me@gmail.com")
    monkeypatch.setenv("AIRFLOW__SMTP__SMTP_PASSWORD", "app-password")

    with mock.patch.object(run_radar.smtplib, "SMTP") as smtp_cls:
        run_radar.send_email(
            to="fan@example.com",
            subject="New releases",
            html_content="<b>hi</b>",
            custom_headers={"List-Unsubscribe": "<https://radar.example.com/unsubscribe/t>"},
        )

    smtp_cls.assert_called_once_with("smtp.gmail.com", 587, timeout=30)
    smtp = smtp_cls.return_value.__enter__.return_value
    smtp.starttls.assert_called_once()
    smtp.login.assert_called_once_with("me@gmail.com", "app-password")
    msg = smtp.send_message.call_args.args[0]
    assert msg["To"] == "fan@example.com"
    assert msg["Subject"] == "New releases"
    assert msg["List-Unsubscribe"] == "<https://radar.example.com/unsubscribe/t>"
    assert "<b>hi</b>" in msg.get_body(preferencelist=("html",)).get_content()


def test_db_uses_registered_connection_factory(monkeypatch):
    conn = object()
    monkeypatch.setattr(db, "_connection_factory", None)
    db.use_connection_factory(lambda: conn)

    assert db._get_conn() is conn
