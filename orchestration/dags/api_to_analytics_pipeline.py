"""Metadata-driven ELT pipeline for the daily-cadence sources (weather, fx
-- control.api_sources.owning_dag = 'daily_pipeline'). Near-real-time
crypto ingestion lives in crypto_realtime_pipeline.py instead, decoupled
from this DAG's once-daily dbt transform.

    get_active_sources                          (query control.api_sources)
        -> extract_and_load  .expand()           (parallel, one mapped task instance per active source)
            -> check_extraction_results          (trigger_rule=ALL_DONE, branches)
                -> [all sources failed]  notify_total_failure
                -> [>=1 succeeded]       dbt_deps -> dbt_run_silver -> dbt_test_silver
                                                  -> dbt_run_gold  -> dbt_test_gold
    -> send_run_summary_email_task               (trigger_rule=ALL_DONE, always runs last)

Adding a 4th API source means adding a row to control.api_sources and one
line in include/extractors/registry.py -- this file doesn't change.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

from utils.metadata import get_active_sources as fetch_active_sources
from utils.metadata import summarize_run
from utils.notifications import send_failure_alert, send_run_summary_email

logger = logging.getLogger(__name__)

DBT_DIR = "/usr/local/airflow/dbt"  # mounted from ../dbt, see docker-compose.override.yml

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "on_failure_callback": send_failure_alert,
}


@dag(
    dag_id="api_to_analytics_pipeline",
    description="Metadata-driven multi-API ingestion -> dbt silver/gold medallion transform",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_tasks=8,
    tags=["ingestion", "dbt", "medallion"],
    default_args=DEFAULT_ARGS,
)
def api_to_analytics_pipeline():
    @task
    def get_active_sources() -> list[dict]:
        sources = fetch_active_sources(owning_dag="daily_pipeline")
        logger.info("Found %d active source(s): %s", len(sources), [s["source_name"] for s in sources])
        return sources

    @task(retries=3, retry_delay=timedelta(seconds=30))
    def extract_and_load(source_config: dict, **context) -> dict:
        from include.extractors.registry import build_extractor

        extractor = build_extractor(
            source_config,
            dag_run_id=context["run_id"],
            task_id=context["task_instance"].task_id,
        )
        return extractor.run()

    @task.branch(trigger_rule=TriggerRule.ALL_DONE)
    def check_extraction_results(**context) -> str:
        summary = summarize_run(context["run_id"])
        logger.info("Extraction summary for run %s: %s", context["run_id"], summary)
        if summary.get("success", 0) == 0:
            return "notify_total_failure"
        return "dbt_deps"

    notify_total_failure = EmptyOperator(task_id="notify_total_failure")

    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="dbt deps --profiles-dir .",
        cwd=DBT_DIR,
    )
    dbt_run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command="dbt run --select tag:silver --profiles-dir . --target ${DBT_TARGET:-dev}",
        cwd=DBT_DIR,
    )
    dbt_test_silver = BashOperator(
        task_id="dbt_test_silver",
        bash_command="dbt test --select tag:silver --profiles-dir . --target ${DBT_TARGET:-dev}",
        cwd=DBT_DIR,
    )
    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command="dbt run --select tag:gold --profiles-dir . --target ${DBT_TARGET:-dev}",
        cwd=DBT_DIR,
    )
    dbt_test_gold = BashOperator(
        task_id="dbt_test_gold",
        bash_command="dbt test --select tag:gold --profiles-dir . --target ${DBT_TARGET:-dev}",
        cwd=DBT_DIR,
    )

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def send_run_summary_email_task(**context):
        summary = summarize_run(context["run_id"])
        send_run_summary_email(dag_id=context["dag"].dag_id, run_id=context["run_id"], summary=summary)

    sources = get_active_sources()
    extraction_results = extract_and_load.expand(source_config=sources)
    branch = check_extraction_results()
    summary_email = send_run_summary_email_task()

    extraction_results >> branch
    branch >> [dbt_deps, notify_total_failure]
    dbt_deps >> dbt_run_silver >> dbt_test_silver >> dbt_run_gold >> dbt_test_gold
    [dbt_test_gold, notify_total_failure] >> summary_email


api_to_analytics_pipeline()
