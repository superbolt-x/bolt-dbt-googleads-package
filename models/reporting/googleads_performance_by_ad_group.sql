{{ config (
    alias = target.database + '_googleads_performance_by_ad_group',
    materialized = 'incremental',
    unique_key = 'unique_key',
    incremental_strategy = 'delete+insert',
    on_schema_change = 'append_new_columns'
)}}

{#-
    Ad group performance, all date granularities in one table.

    Reads the day-grain incremental staging model directly (no redundant
    googleads_ad_groups_insights middle table), applies currency conversion +
    date parts, rolls up day/week/month/quarter/year, and joins ad group +
    campaign + account metadata built inline from the raw history tables.

    Incremental: daily runs reprocess from the start of the year containing
    (max date - googleads_lookback_days). Run --full-refresh weekly to rebuild
    all history and refresh object names/status on older rows.
-#}

{%- set currency_fields = [
    "spend"
]
-%}

{%- set exclude_fields = [
    "unique_key",
    "_fivetran_id",
    "_fivetran_synced",
    "customer_time_zone",
    "campaign",
    "day_of_week",
    "last_updated",
    "ad_group_name",
    "ad_group_status",
    "campaign_id",
    "account_id",
    "account_currency_code",
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
    "active_view_measurable_cost_micros"
]
-%}

{%- set stg_fields = adapter.get_columns_in_relation(ref('_stg_googleads_ad_groups_insights'))
                    |map(attribute="name")
                    |reject("in",exclude_fields)
                    |list
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
    FROM {{ ref('_stg_googleads_ad_groups_insights') }}
    {%- if var('currency') != 'USD' %}
    LEFT JOIN currency USING(date)
    {%- endif %}
    {% if is_incremental() -%}
    where date >= date_trunc('year', (select dateadd(day,-{{ var('googleads_lookback_days', 31) }},max(date)) from {{ this }}))::date
    {%- endif %}
    ),

    insights_stg AS
    (SELECT *,
    {{ get_date_parts('date') }}
    FROM insights),

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
{%- set measure_exclude = ['date','day','week','month','quarter','year','type','last_updated','unique_key','end_date_time','start_date_time'] -%}
{%- set dimensions = ['ad_group_id'] -%}
{%- set measures = stg_fields
                    |reject("in",measure_exclude)
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

    ad_groups AS
    (SELECT campaign_id, ad_group_id, ad_group_name, ad_group_status
    FROM ad_groups_staging
    ),

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
    date||'_'||date_granularity||'_'||ad_group_id as unique_key
FROM
    ({% for date_granularity in date_granularity_list -%}
    SELECT *
    FROM performance_{{date_granularity}}
    {% if not loop.last %}UNION ALL
    {% endif %}

    {%- endfor %}
    )
LEFT JOIN ad_groups USING(ad_group_id)
LEFT JOIN campaigns USING(campaign_id)
LEFT JOIN accounts USING(account_id)
