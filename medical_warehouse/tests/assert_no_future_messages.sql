-- This test fails if any message has a date in the future.
-- Returns rows that violate the rule — dbt expects 0 rows to pass.
select
    message_id,
    channel_name,
    message_date
from {{ ref('stg_telegram_messages') }}
where message_date > now()