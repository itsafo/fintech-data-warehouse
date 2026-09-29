-- Bronze is append-only (potentially multiple polls/day), but weather is
-- still logically "one value per city per day" for the gold layer, so this
-- dedups to the latest observation per day. Postgres has no QUALIFY, hence
-- the wrapping subquery.

with ranked as (
    select
        city,
        latitude,
        longitude,
        temperature_c,
        windspeed_kmh,
        weather_code,
        observed_at,
        captured_at,
        extracted_date,
        row_number() over (
            partition by city, extracted_date
            order by captured_at desc
        ) as rn
    from {{ source('bronze', 'raw_weather') }}
)

select
    city,
    latitude,
    longitude,
    temperature_c,
    windspeed_kmh,
    weather_code,
    observed_at,
    captured_at,
    extracted_date
from ranked
where rn = 1
