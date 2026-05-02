
with data_with_timestamp as (
select
    user_id,
    user_name,
    user_display_name,
    user_gravatar_id,
    user_profile_api_url,
    user_avatar_url,
    min(event_timestamp) as valid_from,
    max(event_timestamp) as valid_to
from {{ ref('stg_all_events') }}
group by user_id, user_name, user_display_name, user_gravatar_id, user_profile_api_url, user_avatar_url
),


max_timestamp as (
select
    *,
    max(valid_to) over (partition by user_id order by valid_to) as max_event_timestamp
from data_with_timestamp
)

select
    user_id,
    user_name,
    user_display_name,
    user_gravatar_id,
    user_profile_api_url,
    user_avatar_url,
    valid_from,
    case when valid_to = max_event_timestamp then CAST(DATE(9999, 12, 31) as TIMESTAMP) else valid_to end as valid_to
from max_timestamp




