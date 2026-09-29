-- Control-plane + bronze landing DDL for the fintech-data-warehouse project.
-- Applied by Terraform (terraform/bootstrap.tf) via a local-exec provisioner
-- once the local_warehouse container and schemas/roles exist. Idempotent:
-- safe to re-run (CREATE ... IF NOT EXISTS, ON CONFLICT DO NOTHING).

CREATE SCHEMA IF NOT EXISTS control;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

-- =====================================================================
-- CONTROL PLANE: metadata registry + observability logs
-- =====================================================================

CREATE TABLE IF NOT EXISTS control.api_sources (
    source_id            SERIAL PRIMARY KEY,
    source_name          VARCHAR(100) NOT NULL UNIQUE,
    base_url             VARCHAR(255) NOT NULL,
    endpoint_path        VARCHAR(255) NOT NULL,
    http_method          VARCHAR(10)  NOT NULL DEFAULT 'GET',
    auth_type            VARCHAR(20)  NOT NULL DEFAULT 'none'
                          CHECK (auth_type IN ('none', 'api_key', 'bearer')),
    auth_conn_id         VARCHAR(100),              -- Airflow Connection id, NULL when auth_type = 'none'
    query_params         JSONB        NOT NULL DEFAULT '{}'::jsonb,
    target_bronze_table  VARCHAR(100) NOT NULL,     -- e.g. 'bronze.raw_weather'
    load_type            VARCHAR(20)  NOT NULL DEFAULT 'full'
                          CHECK (load_type IN ('full', 'incremental')),
    watermark_column     VARCHAR(100),
    last_extracted_at    TIMESTAMPTZ,
    is_active            BOOLEAN      NOT NULL DEFAULT TRUE,
    schedule_cron        VARCHAR(50),               -- informational; actual schedule lives on the DAG
    owning_dag           VARCHAR(100) NOT NULL DEFAULT 'daily_pipeline'
                          CHECK (owning_dag IN ('daily_pipeline', 'crypto_realtime_pipeline')),
    created_at           TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS control.pipeline_run_log (
    run_log_id        BIGSERIAL PRIMARY KEY,
    dag_run_id        VARCHAR(255) NOT NULL,
    task_id           VARCHAR(255) NOT NULL,
    source_id         INT REFERENCES control.api_sources(source_id),
    status            VARCHAR(20)  NOT NULL CHECK (status IN ('success', 'failed')),
    rows_loaded       INT,
    started_at        TIMESTAMPTZ  NOT NULL,
    ended_at          TIMESTAMPTZ  NOT NULL,
    duration_seconds  NUMERIC      GENERATED ALWAYS AS (EXTRACT(EPOCH FROM (ended_at - started_at))) STORED
);

CREATE INDEX IF NOT EXISTS idx_pipeline_run_log_dag_run_id ON control.pipeline_run_log(dag_run_id);

CREATE TABLE IF NOT EXISTS control.error_log (
    error_log_id      BIGSERIAL PRIMARY KEY,
    dag_run_id        VARCHAR(255) NOT NULL,
    task_id           VARCHAR(255) NOT NULL,
    source_id         INT REFERENCES control.api_sources(source_id),
    error_message     TEXT NOT NULL,
    error_traceback   TEXT,
    occurred_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_error_log_dag_run_id ON control.error_log(dag_run_id);

-- =====================================================================
-- BRONZE: raw landing tables, written directly by the Python extractors.
-- Append-only by design -- a plain INSERT per poll, never an upsert, so
-- bronze is a genuine immutable time series. captured_at is the real API
-- fetch timestamp; extracted_date is derived from it purely for cheap
-- day-grain filtering/joins downstream. minio_object_key points back to
-- the untouched raw JSON payload archived in the bronze-raw MinIO bucket
-- for this same poll (see orchestration/include/storage/object_store.py).
-- =====================================================================

CREATE TABLE IF NOT EXISTS bronze.raw_weather (
    weather_id       BIGSERIAL PRIMARY KEY,
    city             VARCHAR(100) NOT NULL,
    latitude         NUMERIC(9, 6) NOT NULL,
    longitude        NUMERIC(9, 6) NOT NULL,
    temperature_c    NUMERIC NOT NULL,
    windspeed_kmh    NUMERIC NOT NULL,
    weather_code     INT,
    observed_at      TIMESTAMPTZ NOT NULL,
    captured_at      TIMESTAMPTZ NOT NULL,
    extracted_date   DATE GENERATED ALWAYS AS (captured_at::date) STORED,
    minio_object_key VARCHAR(500)
);

CREATE INDEX IF NOT EXISTS idx_raw_weather_city_date ON bronze.raw_weather(city, extracted_date);

CREATE TABLE IF NOT EXISTS bronze.raw_crypto (
    crypto_id             BIGSERIAL PRIMARY KEY,
    coin_id               VARCHAR(50) NOT NULL,
    vs_currency            VARCHAR(10) NOT NULL DEFAULT 'usdt',
    price                  NUMERIC NOT NULL,
    market_cap             NUMERIC,
    price_change_24h_pct   NUMERIC,
    captured_at             TIMESTAMPTZ NOT NULL,
    extracted_date          DATE GENERATED ALWAYS AS (captured_at::date) STORED,
    minio_object_key        VARCHAR(500)
);

CREATE INDEX IF NOT EXISTS idx_raw_crypto_coin_date ON bronze.raw_crypto(coin_id, vs_currency, extracted_date);
CREATE INDEX IF NOT EXISTS idx_raw_crypto_captured_at ON bronze.raw_crypto(captured_at);

CREATE TABLE IF NOT EXISTS bronze.raw_fx (
    fx_id             BIGSERIAL PRIMARY KEY,
    base_currency     VARCHAR(10) NOT NULL,
    quote_currency    VARCHAR(10) NOT NULL,
    rate              NUMERIC NOT NULL,
    captured_at       TIMESTAMPTZ NOT NULL,
    extracted_date    DATE GENERATED ALWAYS AS (captured_at::date) STORED,
    minio_object_key  VARCHAR(500)
);

CREATE INDEX IF NOT EXISTS idx_raw_fx_pair_date ON bronze.raw_fx(base_currency, quote_currency, extracted_date);

-- =====================================================================
-- SEED: three free, no-auth demo API sources.
-- open_meteo/frankfurter don't meaningfully change faster than daily
-- (Open-Meteo's model updates hourly at best, Frankfurter is a once-daily
-- ECB reference rate), so they stay on daily_pipeline. binance prices move
-- continuously and its free ticker API has generous rate limits, so it's
-- polled every 5 minutes by crypto_realtime_pipeline instead.
-- =====================================================================

INSERT INTO control.api_sources
    (source_name, base_url, endpoint_path, http_method, auth_type, query_params, target_bronze_table, load_type, schedule_cron, owning_dag)
VALUES
    (
        'open_meteo',
        'https://api.open-meteo.com',
        '/v1/forecast',
        'GET',
        'none',
        '{"cities": [
            {"name": "London",   "lat": 51.5074, "lon": -0.1278},
            {"name": "New York", "lat": 40.7128, "lon": -74.0060},
            {"name": "Tokyo",    "lat": 35.6895, "lon": 139.6917}
        ]}'::jsonb,
        'bronze.raw_weather',
        'full',
        '@daily',
        'daily_pipeline'
    ),
    (
        'binance',
        'https://api.binance.com',
        '/api/v3/ticker/24hr',
        'GET',
        'none',
        '{"symbols": ["BTCUSDT", "ETHUSDT", "SOLUSDT"]}'::jsonb,
        'bronze.raw_crypto',
        'full',
        '*/5 * * * *',
        'crypto_realtime_pipeline'
    ),
    (
        'frankfurter',
        'https://api.frankfurter.app',
        '/latest',
        'GET',
        'none',
        '{"base": "USD", "symbols": ["GBP", "EUR", "JPY"]}'::jsonb,
        'bronze.raw_fx',
        'full',
        '@daily',
        'daily_pipeline'
    )
ON CONFLICT (source_name) DO NOTHING;
