-- dim_user for the public role: user_key, actor_id and actor_login masked with mask_pii (salted
-- SHA-256, the silver formula). Same salt on every masked key, so public fct_events still joins
-- public dim_user on user_key. is_bot and the SCD2 columns are kept as-is.
--
-- Incremental like dim_user: a run masks and writes the gold rows loaded after the newest
-- loaded_at already in this table (every version of each actor dim_user recomputed since). A new
-- PII_HASH_SALT or masking rule needs --full-refresh (docs/dbt-models.md).
{{ config(
    materialized='incremental',
    incremental_strategy=('merge' if target.type == 'databricks' else 'delete+insert'),
    unique_key='user_key',
    on_schema_change='append_new_columns'
) }}

select
    {{ mask_pii('user_key') }} as user_key,
    {{ mask_pii('actor_id') }} as actor_id,
    {{ mask_pii('actor_login') }} as actor_login,
    version_number,
    valid_from,
    valid_to,
    is_current,
    is_bot,
    loaded_at
from {{ ref('dim_user') }}
{%- if is_incremental() %}
where loaded_at > {{ load_watermark(this) }}
{%- endif %}
