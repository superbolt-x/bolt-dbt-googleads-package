{{ config( 
        materialized='incremental',
        unique_key='unique_key'
) }}

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
    "interactions",
    "engagements",
    "ad_group_id",
    "ad_group_name",
    "info_text"
]
-%}

{%- set stg_fields = adapter.get_columns_in_relation(ref('_stg_googleads_searchterms_insights'))
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
    FROM {{ ref('_stg_googleads_searchterms_insights') }}
    {%- if var('currency') != 'USD' %}
    LEFT JOIN currency USING(date)
    {%- endif %}
    {% if is_incremental() -%}

    where date >= (select max(date)-30 from {{ this }})

    {% endif %}
    )

SELECT *,
    keyword_ad_group_criterion||'_'||search_term||'_'||search_term_match_type||'_'||date as unique_key,
    {{ get_date_parts('date') }}
FROM insights 



