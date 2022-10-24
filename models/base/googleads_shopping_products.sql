{%- set selected_fields = [
    "ad_group_id",
    "product_channel",
    "product_item_id",
    "product_title",
    "product_type_l_1",
    "_fivetran_synced"
] -%}
{%- set schema_name, table_name = 'googleads_raw', 'shopping_performance_report' -%}

WITH staging AS 
    (SELECT 
        {% for field in selected_fields|reject("eq","_fivetran_synced") -%}
        {{ get_googleads_clean_field(table_name, field) }}
        {%- if not loop.last %},{%- endif %}
        {% endfor -%}
    FROM 
        (SELECT
            {{ selected_fields|join(", ") }},
            MAX(_fivetran_synced) OVER (PARTITION BY ad_group_id, product_item_id) as product_last_updated_at
        FROM {{ source(schema_name, table_name) }}
        WHERE product_item_id <> '')
    WHERE _fivetran_synced = product_last_updated_at
    )

SELECT 
    ad_group_id,
    product_item_id,
    MAX(product_channel) as product_channel,
    MAX(product_title) as product_title,
    MAX(product_type_l_1) as product_type_l_1,
    ad_group_id||'_'||product_item_id as unique_key
FROM staging 
GROUP BY ad_group_id, product_item_id