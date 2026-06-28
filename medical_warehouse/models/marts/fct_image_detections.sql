with detections as (
    select
        message_id::bigint          as message_id,
        channel_name::text          as channel_name,
        image_path::text            as image_path,
        detected_objects::text      as detected_objects,
        image_category::text        as image_category,
        object_count::integer       as object_count
    from {{ source('raw', 'yolo_detections') }}
),

messages as (
    select
        message_id,
        channel_key,
        date_key,
        views
    from {{ ref('fct_messages') }}
),

channels as (
    select channel_name, channel_key
    from {{ ref('dim_channels') }}
)

select
    d.message_id,
    m.channel_key,
    m.date_key,
    d.channel_name,
    d.image_path,
    d.detected_objects,
    d.image_category,
    d.object_count,
    m.views
from detections d
left join messages  m on d.message_id = m.message_id
left join channels  c on d.channel_name = c.channel_name