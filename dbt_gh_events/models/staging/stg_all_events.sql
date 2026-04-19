

with source as (
    select * from {{ source('github', 'github_events') }}
),

deduplicated_event_ids as (
    select
        *,
        row_number() over (partition by id) as id_row_num
    from source
)

select
    id as event_id,
    type as event_type,
    actor.id as user_id,
    actor.login as user_name,
    actor.display_login as user_display_name,
    actor.gravatar_id as user_gravatar_id,
    actor.url as user_profile_api_url,
    actor.avatar_url as user_avatar_url,
    repo.id as repo_id,
    repo.name as repo_name,
    repo.url as repo_api_url,
    payload as event_payload,
    public as is_public,
    created_at as event_timestamp,
    org.id as organization_id,
    org.login as organization_name,
    org.url as organization_api_url,
    org.gravatar_id as organization_gravatar_id,
    org.avatar_url as organization_avatar_url
from deduplicated_event_ids
where id_row_num = 1



