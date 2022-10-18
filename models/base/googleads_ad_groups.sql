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
    
        {% for field in selected_fields -%}
        {{ get_googleads_clean_field(table_name, field) }},
        {% endfor -%}
        MAX(updated_at) OVER (PARTITION BY id) as last_updated_at

    FROM {{ source(schema_name, table_name) }}
    )

SELECT *
FROM staging 
WHERE updated_at = last_updated_at