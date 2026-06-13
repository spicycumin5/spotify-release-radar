# Astro Runtime images are published at quay.io/astronomer/astro-runtime.
# This project's DAGs use the Airflow 3 Task SDK (`airflow.sdk` for
# @dag/@task/Variable), so the runtime must be Airflow-3-based (Astro
# Runtime 13+). After installing the Astro CLI, run `astro runtime list`
# to confirm the latest available tag and adjust below if needed.
FROM quay.io/astronomer/astro-runtime:13.1.0
