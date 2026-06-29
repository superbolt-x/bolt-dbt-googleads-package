{{ config (
    alias = target.database + '_googleads_performance_by_keyword',
    materialized = 'incremental',
    unique_key = 'unique_key',
    incremental_strategy = 'delete+insert',
    on_schema_change = 'append_new_columns'
)}}

{#-
    Keyword performance, all date granularities in one table.

    Reads the day-grain incremental staging model directly (no redundant
    googleads_keywords_insights middle table), applies currency conversion +
    date parts, rolls up day/week/month/quarter/year, and joins keyword /
    ad group / campaign / account metadata built inline from the raw history
    tables.

    Optional stream — off by default; enable per-client (e.g. Erie) by setting
    +enabled: true on _stg_googleads_keywords_insights and this model.

    Incremental: daily runs reprocess from the start of the year containing
    (max date - googleads_lookback_days). Run --full-refresh weekly to rebuild
    history and refresh object names/status on older rows.
-#}

{%- set currency_fields = [
    "spend"
]
-%}

{%- set exclude_fields = [
    "unique_key",
    "_fivetran_id",
    "_fivetran_synced",
    "last_updated",
    "account_id",
    "account_name",
    "account_currency_code",
    "campaign_id",
    "campaign_name",
    "ad_group_name",
    "keyword_status",
    "keyword_text",
    "keyword_match_type",
    "ad_group_criterion_approval_status",
    "interactions",
    "engagements",
    "bounce_rate",
    "gmail_forwards",
    "gmail_saves",
    "gmail_secondary_clicks",
    "video_quartile_p_25_rate",
    "active_view_measurable_cost_micros",
    "quality_info_creative_quality_score",
    "quality_info_post_click_quality_score",
    "quality_info_quality_score",
    "quality_info_search_predicted_ctr"
]
-%}

{%- set stg_fields = adapter.get_columns_in_relation(ref('_stg_googleads_keywords_insights'))
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
    FROM {{ ref('_stg_googleads_keywords_insights') }}
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
    "ad_group_id",
    "id",
    "keyword_match_type",
    "keyword_text",
    "negative",
    "status",
    "updated_at"
] -%}
{%- set schema_name, table_name = 'googleads_raw', 'keywords' -%}

    keywords_staging AS
    (SELECT
        {% for field in selected_fields|reject("eq","updated_at") -%}
        {{ get_googleads_clean_field(table_name, field) }}
        {%- if not loop.last %},{%- endif %}
        {% endfor -%}
    FROM
        (SELECT
            {{ selected_fields|join(", ") }},
            MAX(updated_at) OVER (PARTITION BY ad_group_id, id) as last_updated_at
        FROM {{ source(schema_name, table_name) }})
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
{%- set measure_exclude = ['date','day','week','month','quarter','year','last_updated','unique_key','end_date_time','start_date_time','quality_info_creative_score','quality_info_post_click_score','quality_info_score','quality_info_search_predicted_ctr'] -%}
{%- set dimensions = ['ad_group_id','keyword_id'] -%}
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

    keywords AS
    (SELECT ad_group_id, keyword_id, keyword_text, keyword_match_type, keyword_status, keyword_negative
    FROM keywords_staging
    ),

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
    date||'_'||date_granularity||'_'||ad_group_id||'_'||keyword_id as unique_key
FROM
    ({% for date_granularity in date_granularity_list -%}
    SELECT *
    FROM performance_{{date_granularity}}
    {% if not loop.last %}UNION ALL
    {% endif %}

    {%- endfor %}
    )
LEFT JOIN keywords USING(ad_group_id, keyword_id)
LEFT JOIN ad_groups USING(ad_group_id)
LEFT JOIN campaigns USING(campaign_id)
LEFT JOIN accounts USING(account_id)
