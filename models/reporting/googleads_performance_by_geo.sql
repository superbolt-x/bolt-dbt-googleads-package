{{ config (
    alias = target.database + '_googleads_performance_by_geo'
)}}

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}
{%- set exclude_fields = ['date','day','week','month','quarter','year','last_updated','unique_key'] -%}
{%- set dimensions = ['campaign_id','geo_target_state','geo_target_city','geo_target_metro'] -%}
{%- set measures = adapter.get_columns_in_relation(ref('googleads_geo_insights'))
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
    FROM {{ ref('googleads_geo_insights') }}
    GROUP BY {{ range(1, dimensions|length +2 +1)|list|join(',') }}
    ),
    {%- endfor %}

    campaigns AS 
    (SELECT {{ dbt_utils.star(from = ref('googleads_campaigns'), except = ["unique_key"]) }}
    FROM {{ ref('googleads_campaigns') }}
    ),

    accounts AS 
    (SELECT {{ dbt_utils.star(from = ref('googleads_accounts'), except = ["unique_key"]) }}
    FROM {{ ref('googleads_accounts') }}
    )

SELECT *,
    {{ get_googleads_default_campaign_types('campaign_name')}},
    date||'_'||date_granularity||'_'||campaign_id||geo_target_state||'_'||geo_target_city||'_'||geo_target_metro as unique_key
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
