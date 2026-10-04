FROM quay.io/astronomer/astro-runtime:12.1.0

# dbt lives in its own virtualenv: installing dbt-core alongside Airflow's
# pinned dependencies makes pip's resolver give up (ResolutionTooDeep). The
# DAGs call it by absolute path (DBT_BIN in api_to_analytics_pipeline.py).
RUN python -m venv /usr/local/airflow/dbt_venv \
    && /usr/local/airflow/dbt_venv/bin/pip install --no-cache-dir \
        "dbt-core==1.8.*" "dbt-postgres==1.8.*"
