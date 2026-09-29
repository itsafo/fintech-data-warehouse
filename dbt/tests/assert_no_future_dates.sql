-- Singular data-contract test: fails (returns rows) if any staging model
-- has an extracted_date in the future, which would indicate a clock skew
-- or malformed API response upstream.

select 'stg_weather' as source_table, extracted_date
from {{ ref('stg_weather') }}
where extracted_date > current_date

union all

select 'stg_crypto_prices' as source_table, extracted_date
from {{ ref('stg_crypto_prices') }}
where extracted_date > current_date

union all

select 'stg_fx_rates' as source_table, extracted_date
from {{ ref('stg_fx_rates') }}
where extracted_date > current_date
