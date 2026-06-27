with channel_stats as (
    select
        channel_name,
        min(message_date)       as first_post_date,
        max(message_date)       as last_post_date,
        count(*)                as total_posts,
        avg(views)              as avg_views,
        sum(case when has_image then 1 else 0 end) as total_images
    from {{ ref('stg_telegram_messages') }}
    group by channel_name
)

select
    {{ dbt_utils.generate_surrogate_key(['channel_name']) }} as channel_key,
    channel_name,
    case
        when lower(channel_name) like '%pharma%' then 'Pharmaceutical'
        when lower(channel_name) like '%cosmet%' then 'Cosmetics'
        else 'Medical'
    end                         as channel_type,
    first_post_date,
    last_post_date,
    total_posts,
    round(avg_views::numeric, 2) as avg_views,
    total_images
from channel_stats