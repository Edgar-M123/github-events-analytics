
with events as (
    select
        *
    from {{ ref("stg_all_events") }}
)

select
    user_id,
    date(event_timestamp) as date,
    sum(case when event_type = 'PullRequestEvent' then 1 else 0 end) as num_pull_requests,
    sum(case when event_type = 'PushEvent' then 1 else 0 end) as num_pushes
from events
group by user_id, date(event_timestamp)
