"""Unit tests for include.alerting.on_failure_callback (no live email/DB)."""

import pendulum

from include.alerting import on_failure_callback


def _make_context(exception=ValueError("boom")):
    ti = type("TaskInstance", (), {})()
    ti.dag_id = "spotify_release_radar"
    ti.task_id = "fetch_and_diff_releases"
    ti.log_url = "http://localhost:8080/log"

    return {
        "ti": ti,
        "logical_date": pendulum.datetime(2024, 1, 1, tz="UTC"),
        "exception": exception,
    }


def test_on_failure_callback_logs_alert_and_sends_email(mocker):
    mock_log_alert = mocker.patch("include.alerting.db.log_alert")
    mock_send_email = mocker.patch("include.alerting.send_email")
    mocker.patch.dict("os.environ", {"ALERT_EMAIL_TO": "me@example.com"})

    on_failure_callback(_make_context())

    mock_log_alert.assert_called_once()
    log_kwargs = mock_log_alert.call_args.kwargs
    assert log_kwargs["dag_id"] == "spotify_release_radar"
    assert log_kwargs["task_id"] == "fetch_and_diff_releases"
    assert "boom" in log_kwargs["error_message"]
    assert log_kwargs["log_url"] == "http://localhost:8080/log"

    mock_send_email.assert_called_once()
    email_kwargs = mock_send_email.call_args.kwargs
    assert email_kwargs["to"] == "me@example.com"
    assert "spotify_release_radar" in email_kwargs["subject"]
    assert "fetch_and_diff_releases" in email_kwargs["subject"]
    assert "boom" in email_kwargs["html_content"]
