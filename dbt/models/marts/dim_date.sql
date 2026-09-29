with spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2024-01-01' as date)",
        end_date="cast(current_date + interval '1 year' as date)"
    ) }}
)

select
    cast(date_day as date)                    as date_day,
    extract(year from date_day)::int          as year,
    extract(month from date_day)::int         as month,
    extract(day from date_day)::int           as day,
    extract(dow from date_day)::int           as day_of_week,
    trim(to_char(date_day, 'Day'))            as day_name,
    trim(to_char(date_day, 'Month'))          as month_name,
    extract(dow from date_day) in (0, 6)      as is_weekend
from spine
