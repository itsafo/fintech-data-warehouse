"""Reads control.api_sources and control.pipeline_run_log/error_log via the
`local_warehouse` Airflow Connection -- the single source of truth the DAG
fans out over and reports on.
"""
from __future__ import annotations

import psycopg2
import psycopg2.extras
from airflow.hooks.base import BaseHook

WAREHOUSE_CONN_ID = "local_warehouse"


def _connect():
    conn = BaseHook.get_connection(WAREHOUSE_CONN_ID)
    return psycopg2.connect(
        host=conn.host,
        port=conn.port,
        user=conn.login,
        password=conn.password,
        dbname=conn.schema,
    )


def get_active_sources(owning_dag: str) -> list[dict]:
    """owning_dag scopes the metadata table to one DAG's cadence -- e.g. the
    daily weather/fx pipeline only ever sees 'daily_pipeline' rows, so
    crypto_realtime_pipeline's every-5-minutes schedule can't accidentally
    also re-poll them (or vice versa).
    """
    with _connect() as pg_conn, pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            select source_id, source_name, base_url, endpoint_path, http_method,
                   auth_type, auth_conn_id, query_params, target_bronze_table,
                   load_type, watermark_column
            from control.api_sources
            where is_active = true
              and owning_dag = %s
            order by source_id
            """,
            (owning_dag,),
        )
        return [dict(row) for row in cur.fetchall()]


def summarize_run(dag_run_id: str) -> dict:
    """Per-run success/failure counts + total rows loaded, for the summary email."""
    summary = {"success": 0, "failed": 0, "rows_loaded": 0, "error_count": 0}
    with _connect() as pg_conn, pg_conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            select status, count(*) as task_count, coalesce(sum(rows_loaded), 0) as rows_loaded
            from (
                -- Airflow retries log one row per attempt; only the last
                -- attempt per source reflects the run's real outcome.
                select distinct on (source_id) source_id, status, rows_loaded
                from control.pipeline_run_log
                where dag_run_id = %s
                order by source_id, ended_at desc
            ) latest_attempt
            group by status
            """,
            (dag_run_id,),
        )
        for row in cur.fetchall():
            summary[row["status"]] = row["task_count"]
            if row["status"] == "success":
                summary["rows_loaded"] += int(row["rows_loaded"])

        cur.execute(
            "select count(*) as error_count from control.error_log where dag_run_id = %s",
            (dag_run_id,),
        )
        summary["error_count"] = cur.fetchone()["error_count"]

    return summary
