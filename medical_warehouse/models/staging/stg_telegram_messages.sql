with source as (
    select * from {{ source('raw', 'telegram_messages') }}
),

cleaned as (
    select
        message_id::bigint                          as message_id,
        channel_name::text                          as channel_name,
        message_date::timestamptz                   as message_date,
        message_date::date                          as message_date_day,
        coalesce(message_text, '')::text            as message_text,
        length(coalesce(message_text, ''))          as message_length,
        has_media::boolean                          as has_media,
        case when image_path is not null
             then true else false end               as has_image,
        image_path::text                            as image_path,
        coalesce(views, 0)::integer                 as views,
        coalesce(forwards, 0)::integer              as forwards,
        scraped_at::timestamptz                     as scraped_at
    from source
    where message_date is not null
)

select * from cleaned