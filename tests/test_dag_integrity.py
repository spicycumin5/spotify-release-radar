"""Sanity checks that all DAGs import cleanly and are well-formed.

`airflow.models.DagBag` depends on the POSIX-only `fcntl` module, so these
tests don't run on native Windows -- only on Linux/macOS, inside the Astro
Docker container (`astro dev pytest`), or in CI.
"""

import os

import pytest

if os.name != "posix":
    pytest.skip("apache-airflow requires a POSIX OS (Linux/macOS/Docker/WSL2)", allow_module_level=True)

from airflow.models import DagBag

DAGS_FOLDER = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "dags")


def _load_dagbag() -> DagBag:
    return DagBag(dag_folder=DAGS_FOLDER, include_examples=False)


def test_dagbag_imports_without_errors():
    dagbag = _load_dagbag()
    assert not dagbag.import_errors, dagbag.import_errors


def test_expected_dags_present():
    dagbag = _load_dagbag()
    assert "spotify_release_radar" in dagbag.dags
    assert "pipeline_failure_demo" in dagbag.dags


def test_dags_have_owner_and_failure_callback_configured():
    dagbag = _load_dagbag()
    for dag_id, dag in dagbag.dags.items():
        for dag_task in dag.tasks:
            assert dag_task.owner, f"{dag_id}.{dag_task.task_id} has no owner"
            assert dag_task.on_failure_callback is not None, (
                f"{dag_id}.{dag_task.task_id} has no on_failure_callback"
            )
