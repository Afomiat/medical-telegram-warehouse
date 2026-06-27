-- This test fails if any message has a negative view count.
-- Returns rows that violate the rule — dbt expects 0 rows to pass.
select
    message_id,
    channel_name,
    views
from {{ ref('stg_telegram_messages') }}
where views < 0