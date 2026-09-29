-- Full-resolution price history -- the payoff of polling every 5 minutes
-- instead of once/day. Incremental (not a full rebuild each run) since
-- this table only grows; delete+insert on the natural key keeps reruns
-- idempotent even if a watermark window overlaps slightly.
{{
    config(
        materialized='incremental',
        unique_key=['coin_id', 'vs_currency', 'captured_at'],
        incremental_strategy='delete+insert'
    )
}}

select
    coin_id,
    vs_currency,
    price,
    market_cap,
    price_change_24h_pct,
    captured_at,
    extracted_date
from {{ ref('stg_crypto_prices') }}

{% if is_incremental() %}
where captured_at > (select coalesce(max(captured_at), '1970-01-01'::timestamptz) from {{ this }})
{% endif %}
