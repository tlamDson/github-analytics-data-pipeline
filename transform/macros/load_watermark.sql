{#-
  load_watermark(relation): SQL scalar, the newest loaded_at in relation (1970-01-01 when it is
  empty). A relation built before it had the column gives 1970-01-01 too, so the next run
  recomputes everything. The column must not be referenced there: inside the subquery loaded_at
  would silently bind to the outer query.
-#}
{% macro load_watermark(relation) -%}
    {%- set columns = adapter.get_columns_in_relation(relation) -%}
    {%- if 'loaded_at' in (columns | map(attribute='name') | map('lower') | list) -%}
        (select coalesce(max(loaded_at), cast('1970-01-01' as timestamp)) from {{ relation }})
    {%- else -%}
        cast('1970-01-01' as timestamp)
    {%- endif -%}
{%- endmacro %}
