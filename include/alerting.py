"""Failure alerting framework.

Wired into every DAG in this project via default_args["on_failure_callback"].
When any task fails, this:

1. Records an audit row in spotify_radar.alert_log (dag_id, task_id,
   logical_date, error, log link).
2. Sends a structured HTML email with the same context, so a human gets
   notified immediately without having to dig through the Airflow UI.
"""

from __future__ import annotations

import os

from airflow.utils.email import send_email

from include import db


def on_failure_callback(context: dict) -> None:
    ti = context["ti"]
    dag_id = ti.dag_id
    task_id = ti.task_id
    logical_date = context.get("logical_date")
    error_message = str(context.get("exception"))
    log_url = ti.log_url

    db.log_alert(
        dag_id=dag_id,
        task_id=task_id,
        logical_date=logical_date,
        error_message=error_message,
        log_url=log_url,
    )

    html_content = f"""
    <h2>Airflow task failed</h2>
    <table border="1" cellpadding="6">
        <tr><th>DAG</th><td>{dag_id}</td></tr>
        <tr><th>Task</th><td>{task_id}</td></tr>
        <tr><th>Logical date</th><td>{logical_date}</td></tr>
        <tr><th>Error</th><td>{error_message}</td></tr>
        <tr><th>Logs</th><td><a href="{log_url}">{log_url}</a></td></tr>
    </table>
    """
    send_email(
        to=os.environ["ALERT_EMAIL_TO"],
        subject=f"\U0001F6A8 Airflow failure: {dag_id}.{task_id}",
        html_content=html_content,
    )
