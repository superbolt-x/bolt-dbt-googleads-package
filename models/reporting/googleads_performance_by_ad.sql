{{ config (
    alias = target.database + '_googleads_performance_by_ad'
)}}

SELECT *,
    {{ get_date_parts('date') }},
    {{ get_googleads_default_campaign_types('campaign_name')}}

FROM {{ ref('googleads_ads_insights') }}
