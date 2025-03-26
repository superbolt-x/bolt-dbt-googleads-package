{{ config (
    alias = target.database + '_googleads_performance_by_campaign_consolidated'
)}}

{%- set currency_fields = [
    "spend"
]
-%}

{%- set exclude_fields = [
    "unique_key",
    "_fivetran_synced",
    "last_updated"
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

    /* Create a view of insights with proper currency conversion */
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
    ),

    /* Add date parts (day, week, month, etc.) */
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

    /* Get campaign data */
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

    /* Get account data */
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
{%- set base_columns = adapter.get_columns_in_relation(ref('_stg_googleads_campaigns_insights')) %}

    /* Manual list of boolean fields */
    {% set boolean_fields = [
        'campaign_budget_explicitly_shared',
        'active_view_measurability',
        'active_view_viewability',
        'has_recommended_budget' 
    ] %}

    /* Generate aggregation queries for each date granularity */
    {%- for date_granularity in date_granularity_list %}

    performance_{{date_granularity}} AS 
    (SELECT 
        '{{date_granularity}}' as date_granularity,
        {{date_granularity}} as date,
        campaign_id,
        account_id,
        
        /* Measure fields that need special handling */
        {% for boolean_field in boolean_fields %}
            {% if boolean_field in stg_fields|map('lower')|list %}
            BOOL_OR({{ boolean_field }})::INT as {{ boolean_field }},
            {% endif %}
        {% endfor %}

        /* Regular numeric aggregations for common metrics */
        SUM(impressions) as impressions,
        SUM(clicks) as clicks,
        SUM(spend) as spend,
        SUM(conversions) as conversions,
        SUM(conversions_value) as conversions_value,
        SUM(all_conversions) as all_conversions,
        SUM(all_conversions_value) as all_conversions_value,
        
        /* Handle any other fields by checking if they exist */
        {% for col in base_columns %}
            {% set col_name = col.name|lower %}
            {% if col_name not in ['date', 'campaign_id', 'account_id', 'impressions', 'clicks', 'spend', 'conversions', 'conversions_value', 'all_conversions', 'all_conversions_value', 'unique_key', '_fivetran_synced', 'last_updated'] %}
                {% if col_name not in boolean_fields %}
                    {% if loop.index > 1 %},{% endif %}
                    SUM({{ col_name }}) as {{ col_name }}
                {% endif %}
            {% endif %}
        {% endfor %}
    FROM insights_stg
    GROUP BY 1, 2, 3, 4
    ),
    {%- endfor %}

    /* Simplified dimension tables */
    campaigns AS 
    (SELECT account_id, campaign_id, campaign_name, campaign_status, advertising_channel_type
    FROM campaigns_staging
    ),

    accounts AS 
    (SELECT account_id, account_name, account_currency_code
    FROM accounts_staging
    )

/* Final output query */
SELECT *,
    {{ get_googleads_default_campaign_types('campaign_name')}},
    date||'_'||date_granularity||'_'||campaign_id as unique_key
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