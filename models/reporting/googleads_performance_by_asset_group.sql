{{ config (
    alias = target.database + '_googleads_performance_by_asset_group'
)}}

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}
{%- set exclude_fields = ['date','day','week','month','quarter','year','last_updated','unique_key'] -%}
{%- set dimensions = ['asset_group_id'] -%}
{%- set measures = adapter.get_columns_in_relation(ref('googleads_asset_groups_insights'))
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
    FROM {{ ref('googleads_asset_groups_insights') }}
    GROUP BY {{ range(1, dimensions|length +2 +1)|list|join(',') }}
    ),
    {%- endfor %}

    asset_group AS 
    (SELECT {{ dbt_utils.star(from = ref('googleads_asset_groups'), except = ["unique_key"]) }}
    FROM {{ ref('googleads_asset_groups') }}
    ),

   campaigns AS 
    (SELECT {{ dbt_utils.star(from = ref('googleads_campaigns'), except = ["unique_key"]) }}
    FROM {{ ref('googleads_campaigns') }}
    ),
  
    accounts AS 
    (SELECT {{ dbt_utils.star(from = ref('googleads_accounts'), except = ["unique_key"]) }}
    FROM {{ ref('googleads_accounts') }}
    )

SELECT *,
    {{ get_googleads_default_campaign_types('asset_group_name')}},
    date||'_'||date_granularity||'_'||asset_group_id as unique_key
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