{{ config (
    alias = target.database + '_googleads_performance_by_campaign_product'
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
    "campaign_name",
    "campaign_start_date",
    "campaign_end_date",
    "campaign_base_campaign",
    "campaign_campaign_budget",
    "campaign_start_date",
    "campaign_end_date",
    "campaign_status",
    "campaign_serving_status",
    "campaign_status",
    "campaign_advertising_channel_type",
    "campaign_advertising_channel_sub_type",
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
    "last_updated",
    "_fivetran_synced"
]
-%}

{%- set stg_fields = adapter.get_columns_in_relation(ref('_stg_googleads_campaign_products_insights'))
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
    FROM {{ ref('_stg_googleads_campaign_products_insights') }}
    {%- if var('currency') != 'USD' %}
    LEFT JOIN currency USING(date)
    {%- endif %}
    ),

    insights_stg AS 
    (SELECT *,
    {{ get_date_parts('date') }}
    FROM insights),

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
{%- set dimensions = ['campaign_id','product_item_id','product_title'] -%}
{%- set measures = adapter.get_columns_in_relation(ref('googleads_campaign_products_insights'))
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
    GROUP BY {{ range(1, dimensions|length +2 +1)|list|join(',') }}
    ),
    {%- endfor %}

    campaigns AS 
    (SELECT account_id, campaign_id, campaign_name, campaign_status, advertising_channel_type
    FROM campaigns_staging
    ),

    accounts AS 
    (SELECT account_id, account_name, account_currency_code
    FROM accounts_staging
    )

SELECT *,
    {{ get_googleads_default_campaign_types('campaign_name')}},
    date||'_'||date_granularity||'_'||campaign_id||'_'||product_item_id||'_'||product_title as unique_key
FROM 
    ({% for date_granularity in date_granularity_list -%}
    SELECT *
    FROM performance_{{date_granularity}}
    {% if not loop.last %}UNION ALL
    {% endif %}

    {%- endfor %}
    )
LEFT JOIN campaigns USING(campaign_id)
LEFT JOIN accounts USING(account_id)
