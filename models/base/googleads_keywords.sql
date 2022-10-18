{%- set selected_fields = [
    "ad_group_id",
    "id",
    "keyword_match_type",
    "keyword_text",
    "negative",
    "status",
    "cpc_bid_micros",
    "updated_at"
] -%}
{%- set schema_name, table_name = 'googleads_raw', 'keywords' -%}

WITH staging AS 
    (SELECT
    
        {% for field in selected_fields -%}
        {{ get_googleads_clean_field(table_name, field) }},
        {% endfor -%}
        MAX(updated_at) OVER (PARTITION BY ad_group_id, id) as last_updated_at

    FROM {{ source(schema_name, table_name) }}
    )

SELECT *,
    ad_group_id||'_'||keyword_id as unique_key
FROM staging 
WHERE updated_at = last_updated_at