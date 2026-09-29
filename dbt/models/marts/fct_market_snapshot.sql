select
    snapshot.snapshot_date,
    date_dim.year,
    date_dim.month,
    date_dim.day_name,
    date_dim.is_weekend,
    snapshot.avg_temperature_c,
    snapshot.avg_windspeed_kmh,
    snapshot.cities_reported,
    snapshot.btc_price_usd,
    snapshot.eth_price_usd,
    snapshot.sol_price_usd,
    snapshot.usd_to_gbp,
    snapshot.usd_to_eur,
    snapshot.usd_to_jpy
from {{ ref('int_market_snapshot_joined') }} as snapshot
left join {{ ref('dim_date') }} as date_dim
    on snapshot.snapshot_date = date_dim.date_day
