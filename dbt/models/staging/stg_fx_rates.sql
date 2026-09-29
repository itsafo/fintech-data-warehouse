-- Same latest-per-day dedup as stg_weather -- see that file's comment.
-- Frankfurter only actually publishes once/day (ECB reference rates), so
-- in practice there's rarely more than one row to dedup here, but the
-- logic stays correct regardless of how often the source is polled.

with ranked as (
    select
        base_currency,
        quote_currency,
        rate,
        captured_at,
        extracted_date,
        row_number() over (
            partition by base_currency, quote_currency, extracted_date
            order by captured_at desc
        ) as rn
    from {{ source('bronze', 'raw_fx') }}
)

select
    base_currency,
    quote_currency,
    rate,
    captured_at,
    extracted_date
from ranked
where rn = 1
