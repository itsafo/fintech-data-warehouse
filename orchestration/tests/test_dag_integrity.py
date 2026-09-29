"""DAG integrity tests, run in CI (see .github/workflows/airflow_dag_tests.yml)."""
from airflow.models import DagBag


def _load_dag_bag() -> DagBag:
    return DagBag(dag_folder="dags", include_examples=False)


def test_no_import_errors():
    dag_bag = _load_dag_bag()
    assert not dag_bag.import_errors, f"DAG import errors: {dag_bag.import_errors}"


def test_pipeline_dag_structure():
    dag_bag = _load_dag_bag()
    dag = dag_bag.get_dag("api_to_analytics_pipeline")
    assert dag is not None, "api_to_analytics_pipeline did not load"

    expected_task_ids = {
        "get_active_sources",
        "extract_and_load",
        "check_extraction_results",
        "notify_total_failure",
        "dbt_deps",
        "dbt_run_silver",
        "dbt_test_silver",
        "dbt_run_gold",
        "dbt_test_gold",
        "send_run_summary_email_task",
    }
    assert expected_task_ids.issubset(set(dag.task_ids))
    assert dag.catchup is False
    assert dag.schedule_interval == "@daily"


def test_crypto_realtime_dag_structure():
    dag_bag = _load_dag_bag()
    dag = dag_bag.get_dag("crypto_realtime_pipeline")
    assert dag is not None, "crypto_realtime_pipeline did not load"

    expected_task_ids = {"get_active_sources", "extract_and_load"}
    assert expected_task_ids.issubset(set(dag.task_ids))
    # No dbt tasks here on purpose -- this DAG is extract+load only.
    assert not {"dbt_deps", "dbt_run_silver", "dbt_run_gold"} & set(dag.task_ids)
    assert dag.catchup is False
    assert dag.schedule_interval == "*/5 * * * *"
    assert dag.max_active_runs == 1
