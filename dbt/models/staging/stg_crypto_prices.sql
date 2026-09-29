-- Deliberately NOT deduped -- full intraday grain is the entire point of
-- polling every 5 minutes (see crypto_realtime_pipeline). Downstream:
-- int_market_snapshot_joined collapses this to one "closing price" per
-- day for the daily snapshot narrative; fct_crypto_price_ticks preserves
-- every observation for real time-series analysis.

select
    coin_id,
    vs_currency,
    price,
    market_cap,
    price_change_24h_pct,
    captured_at,
    extracted_date
from {{ source('bronze', 'raw_crypto') }}
