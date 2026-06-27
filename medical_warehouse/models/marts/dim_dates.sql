with date_spine as (
    select
        generate_series(
            min(message_date_day),
            max(message_date_day),
            interval '1 day'
        )::date as date_day
    from {{ ref('stg_telegram_messages') }}
)

select
    to_char(date_day, 'YYYYMMDD')::integer  as date_key,
    date_day                                as full_date,
    extract(dow from date_day)::integer     as day_of_week,
    to_char(date_day, 'Day')               as day_name,
    extract(week from date_day)::integer    as week_of_year,
    extract(month from date_day)::integer   as month,
    to_char(date_day, 'Month')             as month_name,
    extract(quarter from date_day)::integer as quarter,
    extract(year from date_day)::integer    as year,
    case when extract(dow from date_day) in (0, 6)
         then true else false end           as is_weekend
from date_spine