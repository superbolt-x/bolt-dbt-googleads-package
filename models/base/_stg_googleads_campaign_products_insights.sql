{{ config( 
        materialized='incremental',
        unique_key='unique_key'
) }}


{%- set schema_name, insights_table_name, convtype_table_name = 'googleads_raw', 'campaign_product_performance_report', 'campaign_product_convtype_performance_report' -%}
{%- set insights_exclude_fields = [
   "_fivetran_id",
    "customer_time_zone",
    "serving_status",
    "base_campaign",
    "percent_new_visitors",
    "day_of_week",
    "advertising_channel_sub_type",
    "start_date",
    "end_date",
    "campaign_budget_explicitly_shared"
]
-%}
{%- set convtype_include_fields = [
    "date",
    "campaign_id",
    "product_item_id",
    "product_title",
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

WITH insights AS 
    (SELECT 
        {%- for field in insights_fields %}
        {{ get_googleads_clean_field(insights_table_name, field) }}
        {%- if not loop.last %},{%- endif %}
        {%- endfor %}
    FROM {{ source(schema_name, insights_table_name) }}
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
        campaign_id, 
        product_item_id,
        product_title,
        {% for conversion in conversions -%}
        COALESCE(SUM(CASE WHEN conversion_action_name = '{{conversion}}' THEN {{ var('googleads_conversion_used_by_custom_conversions') }} ELSE 0 END), 0) as "{{get_clean_conversion_name(conversion)}}",
        COALESCE(SUM(CASE WHEN conversion_action_name = '{{conversion}}' THEN {{ var('googleads_conversion_used_by_custom_conversions') }}_value ELSE 0 END), 0) as "{{get_clean_conversion_name(conversion)}}_value"
        {%- if not loop.last %},{%- endif %}
        {% endfor %}
    FROM convtype_raw
    GROUP BY 1,2,3,4   
    )
    {%- endif %}

SELECT *,
    MAX(_fivetran_synced) over (PARTITION BY account_id) as last_updated,
    campaign_id||'_'||product_item_id||'_'||product_title||'_'||date as unique_key
FROM insights
{%- if convtype_table_exists %}
LEFT JOIN convtype USING(date, campaign_id,product_item_id,product_title)
{%- endif %}
{% if is_incremental() -%}

where date >= (select max(date)-30 from {{ this }})

{% endif %}
