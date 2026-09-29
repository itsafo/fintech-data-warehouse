"""Near-real-time extract+load for crypto (control.api_sources.owning_dag =
'crypto_realtime_pipeline' -- currently just binance). Runs every 5 minutes,
appending straight into bronze.raw_crypto; it deliberately does NOT run dbt
on every cycle -- that would rebuild silver/gold every 5 minutes for no
reason. api_to_analytics_pipeline's once-daily dbt run (and
fct_crypto_price_ticks' incremental materialization) picks up everything
this DAG has appended since the last transform.

    get_active_sources          (query control.api_sources, owning_dag='crypto_realtime_pipeline')
        -> extract_and_load  .expand()   (parallel, one mapped task instance per active source)

Per-task failures still hit control.error_log and trigger an email via
on_failure_callback, same as the daily DAG -- there's just no dbt/branch
stage after extraction here.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task

from utils.metadata import get_active_sources as fetch_active_sources
from utils.notifications import send_failure_alert

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
    "on_failure_callback": send_failure_alert,
}


@dag(
    dag_id="crypto_realtime_pipeline",
    description="Near-real-time crypto extract+load (Binance), decoupled from the daily dbt transform",
    schedule="*/5 * * * *",
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    max_active_runs=1,
    max_active_tasks=8,
    tags=["ingestion", "realtime", "crypto"],
    default_args=DEFAULT_ARGS,
)
def crypto_realtime_pipeline():
    @task
    def get_active_sources() -> list[dict]:
        sources = fetch_active_sources(owning_dag="crypto_realtime_pipeline")
        logger.info("Found %d active source(s): %s", len(sources), [s["source_name"] for s in sources])
        return sources

    @task(retries=3, retry_delay=timedelta(seconds=15))
    def extract_and_load(source_config: dict, **context) -> dict:
        from include.extractors.registry import build_extractor

        extractor = build_extractor(
            source_config,
            dag_run_id=context["run_id"],
            task_id=context["task_instance"].task_id,
        )
        return extractor.run()

    sources = get_active_sources()
    extract_and_load.expand(source_config=sources)


crypto_realtime_pipeline()
