{{ config (
    alias = target.database + '_googleads_performance_by_ad'
)}}

{%- set currency_fields = [
    "spend"
]
-%}

{%- set exclude_fields = [
    "unique_key",
    "_fivetran_synced",
    "account_id",
    "account_name",
    "account_currency_code",
    "campaign_id",
    "campaign_name",
    "ad_group_name",
    "ad_status",
    "ad_strength",
    "ad_type",
    "ad_device_preference",
    "ad_added_by_google_ads",
    "ad_final_urls",
    "active_view_measurable_cost_micros",
    "engagements",
    "interactions",
    "bounce_rate",
    "gmail_ad_marketing_image_headline",
    "gmail_ad_marketing_image_description",
    "gmail_forwards",
    "gmail_saves",
    "gmail_secondary_clicks",
    "video_quartile_p_25_rate",
    "ad_display_url",
    "call_ad_description_1",
    "call_ad_description_2",
    "responsive_search_ad_headlines",
    "responsive_search_ad_descriptions",
    "responsive_display_ad_headlines",
    "responsive_display_ad_descriptions",
    "expanded_dynamic_search_ad_description",
    "expanded_text_ad_description",
    "expanded_text_ad_description_2",
    "expanded_text_ad_path_1",
    "expanded_text_ad_path_2",
    "expanded_text_ad_headline_part_1",
    "expanded_text_ad_headline_part_2",
    "expanded_text_ad_headline_part_3",
    "legacy_responsive_display_ad_short_headline",
    "legacy_responsive_display_ad_long_headline",
    "legacy_responsive_display_ad_call_to_action_text",
    "legacy_responsive_display_ad_description",
    "text_ad_headline",
    "text_ad_description_1",
    "ad_text_ad_description_2",
    "ad_responsive_display_ad_long_headline"
]
-%}

{%- set stg_fields = adapter.get_columns_in_relation(ref('_stg_googleads_ads_insights'))
                    |map(attribute="name")
                    |reject("in",exclude_fields)
                    -%}  

WITH 
    {% if var('currency') != 'USD' -%}
    currency AS
    (SELECT DISTINCT date, "{{ var('currency') }}" as raw_rate, 
        LAG(raw_rate) ignore nulls over (order by date) as exchange_rate
    FROM utilities.dates 
    LEFT JOIN utilities.currency USING(date)
    WHERE date <= current_date),
    {%- endif -%}

    {%- set exchange_rate = 1 if var('currency') == 'USD' else 'exchange_rate' %}

    insights AS 
    (SELECT 
        {%- for field in stg_fields -%}
        {%- if field in currency_fields or '_value' in field %}
        "{{ field }}"::float/{{ exchange_rate }} as "{{ field }}"
        {%- else %}
        "{{ field }}"
        {%- endif -%}
        {%- if not loop.last %},{%- endif %}
        {%- endfor %}
    FROM {{ ref('_stg_googleads_ads_insights') }}
    {%- if var('currency') != 'USD' %}
    LEFT JOIN currency USING(date)
    {%- endif %}
    ),

    insights_stg AS 
    (SELECT *,
    {{ get_date_parts('date') }}
    FROM insights),

{%- set ad_selected_fields = [
    "ad_group_id",
    "id",
    "name",
    "status",
    "final_urls",
    "updated_at"
] -%}

{%- set schema_name, ad_table_name = 'googleads_raw', 'ads' -%}

    ads_staging AS 
    (SELECT 
        {% for field in ad_selected_fields|reject("eq","updated_at") -%}
        {{ get_googleads_clean_field(ad_table_name, field) }}
        {%- if not loop.last %},{%- endif %}
        {% endfor -%}
    FROM 
        (SELECT
            {{ ad_selected_fields|join(", ") }},
            MAX(updated_at) OVER (PARTITION BY ad_group_id, id) as last_updated_at
        FROM {{ source(schema_name, ad_table_name) }})
    WHERE updated_at = last_updated_at
    ),

{%- set selected_fields = [
    "campaign_id",
    "id",
    "name",
    "status",
    "updated_at"
] -%}
{%- set schema_name, table_name = 'googleads_raw', 'ad_groups' -%}

    ad_groups_staging AS 
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
    ),

{%- set selected_fields = [
    "customer_id",
    "id",
    "name",
    "status",
    "advertising_channel_type",
    "updated_at"
] -%}
{%- set schema_name, table_name = 'googleads_raw', 'campaigns' -%}

    campaigns_staging AS 
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
    ),

{%- set selected_fields = [
    "id",
    "descriptive_name",
    "currency_code",
    "updated_at"
] -%}
{%- set schema_name, table_name = 'googleads_raw', 'accounts' -%}

    accounts_staging AS 
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
    ),

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}
{%- set exclude_fields = ['date','day','week','month','quarter','year','last_updated','unique_key'] -%}
{%- set dimensions = ['ad_group_id','ad_id'] -%}
{%- set measures = adapter.get_columns_in_relation(ref('googleads_ads_insights'))
                    |map(attribute="name")
                    |reject("in",exclude_fields)
                    |reject("in",dimensions)
                    |list
                    -%}  
 
    {%- for date_granularity in date_granularity_list %}

    performance_{{date_granularity}} AS 
    (SELECT 
        '{{date_granularity}}' as date_granularity,
        {{date_granularity}} as date,
        {%- for dimension in dimensions %}
        {{ dimension }},
        {%-  endfor %}
        {% for measure in measures -%}
        COALESCE(SUM("{{ measure }}"),0) as "{{ measure }}"
        {%- if not loop.last %},{%- endif %}
        {% endfor %}
    FROM insights_stg
    GROUP BY {{ range(1, dimensions|length +2 +1)|list|join(',') }}),
    {%- endfor %}

    ads AS
    (SELECT ad_group_id, ad_id, ad_name, ad_status, ad_final_urls
    FROM ads_staging),
    
    ad_groups AS
    (SELECT ad_group_id, campaign_id, ad_group_name, ad_group_status
    FROM ad_groups_staging),
    
    campaigns AS
    (SELECT account_id, campaign_id, campaign_name, campaign_status, advertising_channel_type
    FROM campaigns_staging),
    
    accounts AS
    (SELECT account_id, account_name, account_currency_code
    FROM accounts_staging)

SELECT *,
    {{ get_googleads_default_campaign_types('campaign_name')}},
    date||'_'||date_granularity||'_'||ad_group_id||'_'||ad_id as unique_key
FROM 
    ({% for date_granularity in date_granularity_list -%}
    SELECT *
    FROM performance_{{date_granularity}}
    {% if not loop.last %}UNION ALL
    {% endif %}

    {%- endfor %}
    )
LEFT JOIN ads USING(ad_group_id, ad_id)
LEFT JOIN ad_groups USING(ad_group_id)
LEFT JOIN campaigns USING(campaign_id)
LEFT JOIN accounts USING(account_id)