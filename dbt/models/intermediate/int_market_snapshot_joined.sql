-- Conforms three unrelated source domains onto a shared daily grain
-- (extracted_date), pivoting each from narrow rows to wide columns so the
-- gold layer can join a single row per day instead of re-deriving this
-- logic downstream.

with weather_daily as (
    select
        extracted_date,
        avg(temperature_c) as avg_temperature_c,
        avg(windspeed_kmh) as avg_windspeed_kmh,
        count(distinct city) as cities_reported
    from {{ ref('stg_weather') }}
    group by extracted_date
),

-- stg_crypto_prices is full intraday grain (unlike weather/fx, which are
-- already deduped to one row/day) -- MAX(price) isn't a meaningful daily
-- metric once there are many points a day, so this takes the last
-- observed price of each day (its "close") instead. usd_* naming below
-- treats USDT as ~1:1 with USD, which is the standard stablecoin convention.
crypto_closing as (
    select distinct on (extracted_date, coin_id)
        extracted_date,
        coin_id,
        price
    from {{ ref('stg_crypto_prices') }}
    where vs_currency = 'usdt'
    order by extracted_date, coin_id, captured_at desc
),

crypto_daily as (
    select
        extracted_date,
        max(case when coin_id = 'BTC' then price end) as btc_price_usd,
        max(case when coin_id = 'ETH' then price end) as eth_price_usd,
        max(case when coin_id = 'SOL' then price end) as sol_price_usd
    from crypto_closing
    group by extracted_date
),

fx_daily as (
    select
        extracted_date,
        max(case when quote_currency = 'GBP' then rate end) as usd_to_gbp,
        max(case when quote_currency = 'EUR' then rate end) as usd_to_eur,
        max(case when quote_currency = 'JPY' then rate end) as usd_to_jpy
    from {{ ref('stg_fx_rates') }}
    where base_currency = 'USD'
    group by extracted_date
)

select
    coalesce(weather_daily.extracted_date, crypto_daily.extracted_date, fx_daily.extracted_date) as snapshot_date,
    weather_daily.avg_temperature_c,
    weather_daily.avg_windspeed_kmh,
    weather_daily.cities_reported,
    crypto_daily.btc_price_usd,
    crypto_daily.eth_price_usd,
    crypto_daily.sol_price_usd,
    fx_daily.usd_to_gbp,
    fx_daily.usd_to_eur,
    fx_daily.usd_to_jpy
from weather_daily
full outer join crypto_daily
    on weather_daily.extracted_date = crypto_daily.extracted_date
full outer join fx_daily
    on coalesce(weather_daily.extracted_date, crypto_daily.extracted_date) = fx_daily.extracted_date
