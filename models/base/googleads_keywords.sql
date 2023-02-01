{%- set selected_fields = [
    "ad_group_id",
    "ad_group_criterion_criterion_id",
    "keyword_match_type",
    "keyword_text",
    "date"
] -%}
{%- set schema_name, table_name = 'googleads_raw', 'keywords' -%}

WITH staging AS 
    (SELECT 
        {% for field in selected_fields|reject("eq","date") -%}
        {{ get_googleads_clean_field(table_name, field) }}
        {%- if not loop.last %},{%- endif %}
        {% endfor -%}
    FROM 
        (SELECT
            {{ selected_fields|join(", ") }},
            MAX(date) OVER (PARTITION BY ad_group_id, ad_group_criterion_criterion_id) as last_updated_at
        FROM {{ source(schema_name, table_name) }})
    WHERE date = last_updated_at
    )

SELECT *,
    ad_group_id||'_'||keyword_id as unique_key
FROM staging 
