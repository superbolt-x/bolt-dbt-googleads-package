{{ config (
    alias = target.database + '_googleads_performance_by_ad'
)}}

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}
{%- set exclude_fields = ['date','day','week','month','quarter','year','last_updated','unique_key'] -%}
{%- set dimensions = ['ad_group_id','ad_id'] -%}
{%- set measures = adapter.get_columns_in_relation(ref('googleads_ads_insights'))
                    |map(attribute="name")
                    |reject("in",exclude_fields)
                    |reject("in",dimensions)
                    |list
                    -%}  

WITH 
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
    FROM {{ ref('googleads_ads_insights') }}
    GROUP BY {{ range(1, dimensions|length +2 +1)|list|join(',') }}
    ),
    {%- endfor %}

    ads AS 
    (SELECT 
        ad_group_id, 
        ad_id, 
        ad_name, 
        ad_status,
        expanded_text_ad_headline_part_1,
        expanded_text_ad_headline_part_2,
        expanded_text_ad_headline_part_3
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

