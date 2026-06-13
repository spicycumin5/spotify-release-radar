"""Pipeline Failure Demo.

A minimal DAG used to exercise the alerting framework
(include.alerting.on_failure_callback) end-to-end:

1. In the Airflow UI, go to Admin -> Variables and set FORCE_FAILURE to
   "true".
2. Trigger this DAG. The single task raises, which fires
   on_failure_callback: a structured failure email is sent and a row is
   written to spotify_radar.alert_log.
3. Set FORCE_FAILURE back to "false" (or delete it) for a normal,
   successful run.
"""

from __future__ import annotations

import pendulum
from airflow.sdk import Variable, dag, task

from include.alerting import on_failure_callback

default_args = {
    "owner": "spotify-radar",
    "retries": 0,
    "on_failure_callback": on_failure_callback,
}


@dag(
    dag_id="pipeline_failure_demo",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    default_args=default_args,
    tags=["spotify", "alerting-demo"],
)
def pipeline_failure_demo():
    @task
    def maybe_fail() -> None:
        if Variable.get("FORCE_FAILURE", default_var="false").lower() == "true":
            raise RuntimeError("Forced failure for alerting framework demo")
        print("FORCE_FAILURE is not set to 'true' -- task succeeded.")

    maybe_fail()


pipeline_failure_demo()
