{%- set selected_fields = [
    "campaign_id",
    "id",
    "name",
    "status",
    "updated_at"
] -%}
{%- set schema_name, table_name = 'googleads_raw', 'ad_groups' -%}

WITH staging AS 
    (SELECT 
        {% for field in selected_fields|reject("eq","updated_at") -%}
        {{ get_googleads_clean_field(table_name, field) }}
        {%- if not loop.last %},{%- endif %}
        {% endfor -%}
    FROM 
        (SELECT
            {{ selected_fields|join(", ") }},
            MAX(updated_at) OVER (PARTITION BY id) as last_updated_at
        FROM {{ source(schema_name, table_name) }})
    WHERE updated_at = last_updated_at
    )

SELECT *,
    ad_group_id as unique_key
FROM staging 
