{%- set currency_fields = [
    "spend"
]
-%}

{%- set exclude_fields = [
    "unique_key",
    "_fivetran_synced",
    "account_id",
    "account_name",
    "customer_currency_code",
    "campaign_id",
    "campaign_name",
    "ad_group_id",
    "ad_group_name",
    "ad_status",
    "ad_strength",
    "ad_device_preference",
    "ad_added_by_google_ads",
    "active_view_measurable_cost_micros",
    "engagements",
    "interactions",
    "bounce_rate",
    "gmail_forwards",
    "gmail_saves",
    "gmail_secondary_clicks",
    "video_quartile_p_25_rate",
    "ad_display_url",
    "call_ad_description_1",
    "call_ad_description_2",
    "responsive_search_ad_descriptions",
    "responsive_display_ad_descriptions",
    "expanded_dynamic_search_ad_description",
    "expanded_text_ad_description",
    "expanded_text_ad_description_2",
    "expanded_text_ad_path_1",
    "expanded_text_ad_path_2",
    "legacy_responsive_display_ad_short_headline",
    "legacy_responsive_display_ad_long_headline",
    "legacy_responsive_display_ad_call_to_action_text",
    "legacy_responsive_display_ad_description",
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

    ads AS 
    (SELECT ad_group_id, ad_id, ad_name, ad_status
    FROM {{ ref('googleads_ads') }}
    ),

    ad_groups AS 
    (SELECT campaign_id, ad_group_id, ad_group_name, ad_group_status
    FROM {{ ref('googleads_ad_groups') }}
    ),

    campaigns AS 
    (SELECT account_id, campaign_id, campaign_name, campaign_status
    FROM {{ ref('googleads_campaigns') }}
    ),

    accounts AS 
    (SELECT account_id, account_name, account_currency_code
    FROM {{ ref('googleads_accounts') }}
    )

SELECT *
FROM insights 
LEFT JOIN ads USING(ad_id)
LEFT JOIN ad_groups USING(ad_group_id)
LEFT JOIN campaigns USING(campaign_id)
LEFT JOIN accounts USING(account_id)


