{%- set currency_fields = [
    "spend"
]
-%}

{%- set exclude_fields = [
    "unique_key",
    "account_id",
    "account_currency_code",
    "campaign_base_campaign",
    "campaign_campaign_budget",
    "campaign_start_date",
    "campaign_end_date",
    "campaign_status",
    "campaign_serving_status",
    "campaign_name",
    "account_name",
    "campaign_status",
    "advertising_channel_type",
    "gmail_saves",
    "gmail_forwards",
    "gmail_secondary_clicks",
    "content_impression_share",
    "content_budget_lost_impression_share",
    "content_rank_lost_impression_share",
    "video_quartile_p_25_rate",
    "campaign_budget_has_recommended_budget",
    "campaign_budget_recommended_budget_amount_micros",
    "campaign_budget",
    "campaign_budget_period",
    "campaign_budget_total_amount_micros",
    "campaign_budget_explicitly_shared",
    "interactions",
    "active_view_measurability",
    "active_view_viewability",
    "active_view_measurable_cost_micros",
    "_fivetran_synced"
]
-%}

{%- set stg_fields = adapter.get_columns_in_relation(ref('_stg_googleads_campaigns_insights'))
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
    FROM {{ ref('_stg_googleads_campaigns_insights') }}
    {%- if var('currency') != 'USD' %}
    LEFT JOIN currency USING(date)
    {%- endif %}
    )

SELECT *,
    {{ get_date_parts('date') }}
FROM insights 


