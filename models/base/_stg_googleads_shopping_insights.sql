{{ config( 
        materialized='incremental',
        unique_key='unique_key'
) }}


{%- set schema_name, insights_table_name, convtype_table_name = 'googleads_raw', 'shopping_performance_report', 'shopping_convtype_performance_report' -%}
{%- set insights_exclude_fields = [
    "_fivetran_id",
    "customer_time_zone",
    "day_of_week",
    "product_channel",
    "product_channel_exclusivity",
    "product_bidding_category_level_1",
    "product_bidding_category_level_2",
    "product_bidding_category_level_3",
    "product_bidding_category_level_4",
    "product_bidding_category_level_5",
    "product_condition",
    "product_country",
    "product_custom_attribute_0",
    "product_custom_attribute_1",
    "product_custom_attribute_2",
    "product_custom_attribute_3",
    "product_custom_attribute_4",
    "product_language",
    "product_type_l_2",
    "product_type_l_3",
    "product_type_l_4",
    "product_type_l_5",
    "product_aggregator_id",
    "product_store_id",
    "product_brand",
    "product_merchant_id"
]
-%}
{%- set insights_measure_fields = [
    "spend",
    "impressions",
    "clicks",
    "conversions",
    "conversions_value",
    "all_conversions",
    "all_conversions_value"
]
-%}
{%- set convtype_include_fields = [
    "date",
    "ad_group_id",
    "product_item_id",
    "conversion_action_name",
    "conversions",
    "conversions_value",
    "all_conversions",
    "all_conversions_value"
]
-%}

{%- set insights_fields = adapter.get_columns_in_relation(source(schema_name, insights_table_name))
                    |map(attribute="name")
                    |reject("in",insights_exclude_fields)
                    -%}  

WITH insights_raw AS 
    (SELECT 
        {%- for field in insights_fields %}
        {{ get_googleads_clean_field(insights_table_name, field) }}
        {%- if not loop.last %},{%- endif %}
        {%- endfor %}
    FROM {{ source(schema_name, insights_table_name) }}
    ),

    insights AS 
    (SELECT 
        date,
        account_id,
        campaign_id,
        ad_group_id,
        product_item_id,
        MAX(MAX(_fivetran_synced)) over (PARTITION BY account_id) as last_updated,
        {%- for field in insights_measure_fields %}
        COALESCE(SUM({{ field }}),0) as {{ field }}
        {%- if not loop.last %},{%- endif %}
        {%- endfor %}
    FROM insights_raw
    GROUP BY 1,2,3,4,5
    )

    {% set convtype_table_exists = bolt_dbt_utils.check_source_exists(schema_name,convtype_table_name) -%}
    {%- if not convtype_table_exists %}

    {%- else -%}
    , convtype_raw AS (
    SELECT {{ convtype_include_fields|join(", ") }}
    FROM {{ source(schema_name, convtype_table_name) }}
    )

    {% set conversions = dbt_utils.get_column_values(source(schema_name,convtype_table_name),'conversion_action_name') -%}
    , convtype AS (
    SELECT 
        date, 
        ad_group_id,
        product_item_id,
        {% for conversion in conversions -%}
        COALESCE(SUM(CASE WHEN conversion_action_name = '{{conversion}}' THEN {{ var('googleads_conversion_used_by_custom_conversions') }} ELSE 0 END), 0) as "{{get_clean_conversion_name(conversion)}}",
        COALESCE(SUM(CASE WHEN conversion_action_name = '{{conversion}}' THEN {{ var('googleads_conversion_used_by_custom_conversions') }}_value ELSE 0 END), 0) as "{{get_clean_conversion_name(conversion)}}_value"
        {%- if not loop.last %},{%- endif %}
        {% endfor %}
    FROM convtype_raw
    GROUP BY 1,2,3
    )
    {%- endif %}

SELECT *,
    ad_group_id||'_'||product_item_id||'_'||date as unique_key
FROM insights
{%- if convtype_table_exists %}
LEFT JOIN convtype USING(date, ad_group_id, product_item_id)
{%- endif %}
{% if is_incremental() -%}

where date >= (select max(date)-30 from {{ this }})

{% endif %}