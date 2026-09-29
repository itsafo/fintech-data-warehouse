"""SMTP alert helpers. Recipients come from the ALERT_EMAIL_TO env var
(comma-separated); actual SMTP host/user/password are configured as
AIRFLOW__SMTP__* env vars (see .env.example / README) and consumed by
Airflow's built-in send_email, so no credentials live in this module.
"""
from __future__ import annotations

import logging
import os

from airflow.utils.email import send_email

logger = logging.getLogger(__name__)


def _alert_recipients() -> list[str]:
    raw = os.environ.get("ALERT_EMAIL_TO", "")
    return [addr.strip() for addr in raw.split(",") if addr.strip()]


def send_failure_alert(context: dict) -> None:
    """DAG/task on_failure_callback -- fires per failed task instance."""
    recipients = _alert_recipients()
    if not recipients:
        logger.warning("ALERT_EMAIL_TO not set; skipping failure email.")
        return

    task_instance = context["task_instance"]
    subject = f"[fintech-data-warehouse] FAILED: {task_instance.task_id} in {context['dag'].dag_id}"
    body = f"""
    <p>Task <b>{task_instance.task_id}</b> failed on run <b>{context['run_id']}</b>.</p>
    <p>Log URL: <a href="{task_instance.log_url}">{task_instance.log_url}</a></p>
    <p>Exception: {context.get('exception')}</p>
    """
    send_email(to=recipients, subject=subject, html_content=body)


def send_run_summary_email(dag_id: str, run_id: str, summary: dict) -> None:
    """Final, always-runs summary for the whole DAG run (trigger_rule=ALL_DONE)."""
    recipients = _alert_recipients()
    if not recipients:
        logger.warning("ALERT_EMAIL_TO not set; skipping summary email.")
        return

    is_clean = summary.get("failed", 0) == 0 and summary.get("error_count", 0) == 0
    status_word = "SUCCESS" if is_clean else "PARTIAL/TOTAL FAILURE"
    subject = f"[fintech-data-warehouse] {status_word}: {dag_id} run {run_id}"
    body = f"""
    <p>Run <b>{run_id}</b> of <b>{dag_id}</b> finished.</p>
    <ul>
      <li>Sources succeeded: {summary.get('success', 0)}</li>
      <li>Sources failed: {summary.get('failed', 0)}</li>
      <li>Total rows loaded: {summary.get('rows_loaded', 0)}</li>
      <li>Errors logged: {summary.get('error_count', 0)}</li>
    </ul>
    """
    send_email(to=recipients, subject=subject, html_content=body)
