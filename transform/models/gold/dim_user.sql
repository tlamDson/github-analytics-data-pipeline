-- SCD2 per actor_id from event history (gaps-and-islands). A version is a run of consecutive
-- events (order created_at, event_id) of one actor_id with the same actor_login. Deterministic:
-- depends only on the events, not on when dbt runs.
--
-- Incremental (delete+insert on user_key, merge on Databricks, like fct_events): a run recomputes
-- every version of each touched actor, i.e. an actor with a staging row loaded after the newest
-- loaded_at already in this table, whether the row is late, a new login or neither. loaded_at is
-- the newest load of the whole actor, so every version of a touched actor carries it and
-- pub_dim_user finds them all. Staging only appends, so an actor never loses a version and no
-- user_key is left behind. append_new_columns lets a normal run add loaded_at to a table built
-- without it (load_watermark then recomputes every actor once).
{{ config(
    materialized='incremental',
    incremental_strategy=('merge' if target.type == 'databricks' else 'delete+insert'),
    unique_key='user_key',
    on_schema_change='append_new_columns'
) }}

with staged as (
    select * from {{ ref('stg_gharchive_events') }}
),

{%- if is_incremental() %}

touched_actors as (
    select distinct actor_id
    from staged
    where loaded_at > {{ load_watermark(this) }}
),
{%- endif %}

ordered as (
    select
        actor_id,
        actor_login,
        created_at,
        -- the first event of an actor always starts version 1, even when its login is null
        -- (null is not distinct from the null lag() returns on the first row)
        (
            row_number() over (partition by actor_id order by created_at, event_id) = 1
            or actor_login is distinct from lag(actor_login) over (
                partition by actor_id order by created_at, event_id
            )
        ) as starts_version,
        event_id,
        loaded_at
    from staged
    {%- if is_incremental() %}
    where actor_id in (select actor_id from touched_actors)
    {%- endif %}
),

numbered as (
    select
        *,
        sum(case when starts_version then 1 else 0 end) over (
            partition by actor_id order by created_at, event_id
            rows between unbounded preceding and current row
        ) as version_number
    from ordered
),

versions as (
    select
        actor_id,
        version_number,
        any_value(actor_login) as actor_login,
        min(created_at) as valid_from,
        max(loaded_at) as loaded_at
    from numbered
    group by actor_id, version_number
)

select
    cast(actor_id as string) || '-' || cast(version_number as string) as user_key,
    cast(actor_id as bigint) as actor_id,
    cast(actor_login as string) as actor_login,
    cast(version_number as bigint) as version_number,
    cast(valid_from as timestamp) as valid_from,
    cast(
        lead(valid_from) over (partition by actor_id order by version_number) as timestamp
    ) as valid_to,
    lead(valid_from) over (partition by actor_id order by version_number) is null as is_current,
    coalesce(
        right(lower(actor_login), 5) = '[bot]' or right(lower(actor_login), 4) = '-bot', false
    ) as is_bot,
    cast(max(loaded_at) over (partition by actor_id) as timestamp) as loaded_at
from versions
