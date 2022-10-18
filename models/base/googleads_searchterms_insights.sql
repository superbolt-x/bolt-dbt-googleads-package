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
    "interactions",
    "engagements",
    "ad_group_name"
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
    ),

    keywords AS 
    (SELECT *,
        'customers/'||account_id::varchar||'/adGroupCriteria/'||ad_group_id::varchar||'~'||keyword_id::varchar as keyword_ad_group_criterion
    FROM {{ ref('googleads_keywords') }}
    LEFT JOIN ad_groups USING(ad_group_id)
    LEFT JOIN campaigns USING(campaign_id)
    LEFT JOIN accounts USING(account_id)
    )

SELECT *
FROM insights 
LEFT JOIN keywords USING(ad_group_id, keyword_ad_group_criterion)



