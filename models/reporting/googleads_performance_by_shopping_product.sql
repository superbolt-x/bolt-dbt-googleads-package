{{ config (
    alias = target.database + '_googleads_performance_by_shopping_product'
)}}

SELECT *,
    {{ get_date_parts('date') }},
    {{ get_googleads_default_campaign_types('campaign_name')}}

FROM {{ ref('googleads_shopping_insights') }}
