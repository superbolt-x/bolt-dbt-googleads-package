{%- set selected_fields = [
    "campaign_id",
    "asset_group_id",
    "asset_group_name",
    "status"
] -%}
{%- set schema_name, table_name = 'googleads_raw', 'asset_group_performance_report' -%}

WITH staging AS (select *, max(date) over (partition by id) as max_update from {{ source(schema_name, table_name) }})

select campaign_id, id as asset_group_id, name as asset_group_name, status as asset_group_status
from staging 
where date = max_update
