{{ config( 
        materialized='incremental',
        unique_key='unique_key'
) }}


{%- set schema_name, insights_table_name, convtype_table_name = 'googleads_raw', 'ad_performance_report', 'ad_convtype_performance_report' -%}
{%- set insights_exclude_fields = [
   "_fivetran_id",
   "day_of_week",
   "customer_time_zone",
   "percent_new_visitors",
    "marketing_image_display_call_to_action_text_color",
    "ad_tracking_url_template",
    "ad_url_custom_parameters",
    "app_ad_html_5_media_bundles",
    "ad_responsive_display_ad_format_setting",
    "ad_responsive_display_ad_allow_flexible_color",
    "ad_responsive_display_ad_main_color",
    "ad_responsive_display_ad_accent_color",
    "ad_responsive_display_ad_promo_text",
    "ad_responsive_display_ad_call_to_action_text",
    "ad_responsive_display_ad_price_prefix",
    "ad_responsive_display_ad_business_name",
    "ad_system_managed_resource_source",
    "ad_final_mobile_urls",
    "app_ad_headlines",
    "app_ad_images",
    "app_ad_mandatory_ad_text",
    "app_ad_descriptions",
    "app_ad_youtube_videos",
    "call_ad_phone_number",
    "call_ad_description_1",
    "call_ad_description_2",
    "marketing_image_display_call_to_action_text",
    "gmail_ad_marketing_image",
    "gmail_ad_header_image",
    "responsive_search_ad_path_1",
    "responsive_search_ad_path_2",
    "responsive_display_ad_square_logo_images",
    "responsive_display_ad_square_marketing_images",
    "responsive_display_ad_marketing_images",
    "responsive_display_ad_logo_images",
    "responsive_display_ad_youtube_videos",
    "image_ad_name",
    "image_ad_pixel_width",
    "image_ad_pixel_height",
    "image_ad_mime_type",
    "image_ad_image_url",
    "legacy_responsive_display_ad_business_name",
    "legacy_responsive_display_ad_accent_color",
    "legacy_responsive_display_ad_allow_flexible_color",
    "legacy_responsive_display_ad_logo_image",
    "legacy_responsive_display_ad_square_marketing_image",
    "legacy_responsive_display_ad_square_logo_image",
    "legacy_responsive_display_ad_marketing_image",
    "legacy_responsive_display_ad_format_setting",
    "legacy_responsive_display_ad_price_prefix",
    "legacy_responsive_display_ad_main_color",
    "legacy_responsive_display_ad_promo_text",
    "teaser_description",
    "teaser_headline",
    "teaser_logo_image",
    "teaser_business_name",
    "policy_summary_review_status",
    "policy_summary_approval_status",
    "policy_summary_policy_topic_entries",
    "start_date",
    "end_date"
]
-%}
{%- set convtype_include_fields = [
    "date",
    "ad_id",
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
        ad_id, 
        {% for conversion in conversions -%}
        COALESCE(SUM(CASE WHEN conversion_action_name = '{{conversion}}' THEN {{ var('googleads_conversion_used_by_custom_conversions') }} ELSE 0 END), 0) as "{{get_clean_conversion_name(conversion)}}",
        COALESCE(SUM(CASE WHEN conversion_action_name = '{{conversion}}' THEN {{ var('googleads_conversion_used_by_custom_conversions') }}_value ELSE 0 END), 0) as "{{get_clean_conversion_name(conversion)}}_value"
        {%- if not loop.last %},{%- endif %}
        {% endfor %}
    FROM convtype_raw
    GROUP BY 1,2   
    )
    {%- endif %}

SELECT *,
    MAX(_fivetran_synced) over (PARTITION BY account_id) as last_updated,
    ad_id||'_'||date as unique_key
FROM insights
{%- if convtype_table_exists %}
LEFT JOIN convtype USING(date, ad_id)
{%- endif %}
{% if is_incremental() -%}

where date >= (select max(date)-30 from {{ this }})

{% endif %}