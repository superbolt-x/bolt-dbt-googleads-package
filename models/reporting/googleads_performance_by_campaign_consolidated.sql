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
{%- set exclude_dimensions = ['date','day','week','month','quarter','year','account_name','account_currency_code','campaign_name','campaign_status','advertising_channel_type'] -%}
{%- set dimensions = ['campaign_id'] -%}

    /* Get column data types to handle boolean fields properly */
    {% set column_data = run_query("
      SELECT column_name, data_type 
      FROM information_schema.columns 
      WHERE table_schema = '" ~ this.schema ~ "' 
      AND table_name = '_stg_googleads_campaigns_insights'
    ") %}
    
    {% if execute %}
      {% set columns = column_data.columns[0].values() %}
      {% set data_types = column_data.columns[1].values() %}
      {% set column_dict = {} %}
      {% for i in range(columns|length) %}
        {% do column_dict.update({columns[i]: data_types[i]}) %}
      {% endfor %}
    {% endif %}

    /* Define measure list based on data types */
    {%- set numeric_measures = [] -%}
    {%- set boolean_measures = [] -%}
    {%- set other_measures = [] -%}
    
    {% if execute %}
      {% for col in adapter.get_columns_in_relation(ref('_stg_googleads_campaigns_insights')) %}
        {% set col_name = col.name %}
        {% if col_name not in exclude_dimensions and col_name not in dimensions and col_name not in exclude_fields %}
          {% if column_dict.get(col_name) in ('integer', 'bigint', 'decimal', 'numeric', 'real', 'double precision') %}
            {% do numeric_measures.append(col_name) %}
          {% elif column_dict.get(col_name) == 'boolean' %}
            {% do boolean_measures.append(col_name) %}
          {% else %}
            {% do other_measures.append(col_name) %}
          {% endif %}
        {% endif %}
      {% endfor %}
    {% endif %}
 
    {%- for date_granularity in date_granularity_list %}

    performance_{{date_granularity}} AS 
    (SELECT 
        '{{date_granularity}}' as date_granularity,
        {{date_granularity}} as date,
        {%- for dimension in dimensions %}
        {{ dimension }},
        {%-  endfor %}
        
        /* Handle numeric measures */
        {%- for measure in numeric_measures %}
        COALESCE(SUM("{{ measure }}"),0) as "{{ measure }}"
        {%- if not loop.last or boolean_measures|length > 0 or other_measures|length > 0 %},{%- endif %}
        {%- endfor %}
        
        /* Handle boolean measures */
        {%- for measure in boolean_measures %}
        BOOL_OR("{{ measure }}") as "{{ measure }}"
        {%- if not loop.last or other_measures|length > 0 %},{%- endif %}
        {%- endfor %}
        
        /* Handle other measures */
        {%- for measure in other_measures %}
        MAX("{{ measure }}") as "{{ measure }}"
        {%- if not loop.last %},{%- endif %}
        {%- endfor %}
        
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