"""Template-method base class for metadata-driven API extractors.

Concrete subclasses implement fetch()/transform()/load() for one API's
shape; run() owns retry/backoff on fetch, archiving the untouched raw
payload to MinIO, appending (never upserting) the parsed rows into bronze,
and writing one row to control.pipeline_run_log (always) plus
control.error_log (on failure) -- every source gets this contract for
free just by subclassing this.
"""
from __future__ import annotations

import abc
import contextlib
import logging
import traceback
from datetime import datetime, timezone

import psycopg2
from airflow.hooks.base import BaseHook
from tenacity import retry, stop_after_attempt, wait_exponential

from include.storage.object_store import archive_raw_payload

logger = logging.getLogger(__name__)

WAREHOUSE_CONN_ID = "local_warehouse"


class BaseExtractor(abc.ABC):
    def __init__(self, source_config: dict, dag_run_id: str, task_id: str):
        self.source_config = source_config
        self.source_id = source_config["source_id"]
        self.source_name = source_config["source_name"]
        self.query_params = source_config.get("query_params") or {}
        self.dag_run_id = dag_run_id
        self.task_id = task_id

    @abc.abstractmethod
    def fetch(self) -> dict | list:
        """Call the external API and return the raw response payload."""

    @abc.abstractmethod
    def transform(self, raw: dict | list) -> list[dict]:
        """Shape the raw payload into rows matching the target bronze table.
        Don't set captured_at/minio_object_key -- run() injects both after
        this returns, once per poll, identical across every row it produces.
        """

    @abc.abstractmethod
    def load(self, conn, rows: list[dict]) -> int:
        """Append rows into the target bronze table (plain INSERT, never
        an upsert -- bronze is an immutable time series). Returns rows written.
        """

    def _connect(self):
        conn = BaseHook.get_connection(WAREHOUSE_CONN_ID)
        return psycopg2.connect(
            host=conn.host,
            port=conn.port,
            user=conn.login,
            password=conn.password,
            dbname=conn.schema,
        )

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=2, min=2, max=30))
    def _fetch_with_retry(self):
        return self.fetch()

    def run(self) -> dict:
        started_at = datetime.now(timezone.utc)
        try:
            raw = self._fetch_with_retry()

            # One immutable raw object per poll, archived before any parsing
            # touches it -- this is the actual bronze record of what the API
            # returned. captured_at is also the natural-key timestamp every
            # row this poll produces shares.
            captured_at = datetime.now(timezone.utc)
            object_key = archive_raw_payload(self.source_name, captured_at, raw)

            rows = self.transform(raw)
            for row in rows:
                row["captured_at"] = captured_at
                row["minio_object_key"] = object_key

            with contextlib.closing(self._connect()) as pg_conn:
                rows_loaded = self.load(pg_conn, rows)
                pg_conn.commit()
        except Exception as exc:
            ended_at = datetime.now(timezone.utc)
            # Bookkeeping must never mask the real failure.
            try:
                self._log_run(started_at, ended_at, status="failed", rows_loaded=None)
                self._log_error(str(exc), traceback.format_exc())
            except Exception:
                logger.exception("Could not write failure bookkeeping for source=%s", self.source_name)
            logger.exception("Extraction failed for source=%s", self.source_name)
            raise

        # Rows are already committed here, so a bookkeeping failure must not
        # raise -- an Airflow retry would re-poll and append duplicate rows.
        ended_at = datetime.now(timezone.utc)
        try:
            self._log_run(started_at, ended_at, status="success", rows_loaded=rows_loaded)
        except Exception:
            logger.exception("Loaded %d rows but could not write run log for source=%s", rows_loaded, self.source_name)
        return {
            "source_name": self.source_name,
            "status": "success",
            "rows_loaded": rows_loaded,
            "minio_object_key": object_key,
        }

    def _log_run(self, started_at, ended_at, status, rows_loaded):
        with contextlib.closing(self._connect()) as pg_conn, pg_conn.cursor() as cur:
            cur.execute(
                """
                insert into control.pipeline_run_log
                    (dag_run_id, task_id, source_id, status, rows_loaded, started_at, ended_at)
                values (%s, %s, %s, %s, %s, %s, %s)
                """,
                (self.dag_run_id, self.task_id, self.source_id, status, rows_loaded, started_at, ended_at),
            )
            pg_conn.commit()

    def _log_error(self, message: str, tb: str):
        with contextlib.closing(self._connect()) as pg_conn, pg_conn.cursor() as cur:
            cur.execute(
                """
                insert into control.error_log
                    (dag_run_id, task_id, source_id, error_message, error_traceback)
                values (%s, %s, %s, %s, %s)
                """,
                (self.dag_run_id, self.task_id, self.source_id, message, tb),
            )
            pg_conn.commit()
