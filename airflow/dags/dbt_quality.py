"""Daily dbt build + data quality checks for Velo.

warm_warehouse -> dbt_deps -> dbt_build (models + all tests, logged to dq_test_history) -> dq_summary
- warm_warehouse starts the SQL warehouse and waits until RUNNING (avoids cold-start failures)
- retries use exponential backoff (2, 4, 8 min)
- a failing task triggers alert_on_failure (where Slack/PagerDuty would be called)
"""

from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import dag

DBT_VENV = "/home/airflow/dbt_venv/bin"
PROJECT = "/opt/velo/dbt/velo"

LATEST_RUN_SUMMARY = (
    "select status, count(*) as tests from velo.gold.dq_test_history "
    "where invocation_id = (select max_by(invocation_id, run_started_at) "
    "from velo.gold.dq_test_history) group by status order by status"
)


def alert_on_failure(context) -> None:
    ti = context["task_instance"]
    print(
        f"ALERT: {ti.dag_id}.{ti.task_id} failed (run {context['run_id']}). "
        "Check velo.gold.dq_test_history for failing tests."
    )


@dag(
    schedule="0 6 * * *",
    start_date=datetime(2026, 10, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,  # never run two dbt builds at once
    tags=["velo", "dbt", "data-quality"],
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
        "retry_exponential_backoff": True,  # 2, 4, 8 minutes
        "max_retry_delay": timedelta(minutes=15),
        "on_failure_callback": alert_on_failure,
    },
)
def dbt_quality():
    warm = BashOperator(
        task_id="warm_warehouse",
        bash_command=f"{DBT_VENV}/python /opt/velo/airflow/scripts/warm_warehouse.py",
        execution_timeout=timedelta(minutes=25),
    )
    deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {PROJECT} && {DBT_VENV}/dbt deps",
    )
    build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {PROJECT} && {DBT_VENV}/dbt build",
        execution_timeout=timedelta(minutes=45),
    )
    summary = BashOperator(
        task_id="dq_summary",
        bash_command=f'cd {PROJECT} && {DBT_VENV}/dbt show --inline "{LATEST_RUN_SUMMARY}"',
        trigger_rule="all_done",  # report quality even if the build failed
    )
    warm >> deps >> build >> summary


dbt_quality()
