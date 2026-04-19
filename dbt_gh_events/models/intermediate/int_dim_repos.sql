
with data_with_timestamp as (
select
    repo_id,
    repo_name,
    repo_api_url,
    min(event_timestamp) as valid_from,
    max(event_timestamp) as valid_to
from {{ ref('stg_all_events') }}
group by repo_id, repo_name, repo_api_url
),


max_timestamp as (
select
    *,
    max(valid_to) over (partition by user_id order by valid_to) as max_event_timestamp
from data_with_timestamp
)

select
    repo_id,
    repo_name,
    repo_api_url,
    valid_from,
    case when valid_to = max_event_timestamp then make_date(9999, 12, 31) else valid_to end as valid_to
from max_timestamp

