with messages as (
    select * from {{ ref('stg_telegram_messages') }}
),

channels as (
    select * from {{ ref('dim_channels') }}
),

dates as (
    select * from {{ ref('dim_dates') }}
)

select
    m.message_id,
    c.channel_key,
    d.date_key,
    m.channel_name,
    m.message_text,
    m.message_length,
    m.views,
    m.forwards,
    m.has_image,
    m.image_path,
    m.message_date,
    m.scraped_at
from messages m
left join channels c on m.channel_name = c.channel_name
left join dates    d on m.message_date_day = d.full_date