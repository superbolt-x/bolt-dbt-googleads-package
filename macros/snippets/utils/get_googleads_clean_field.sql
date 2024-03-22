{%- macro get_googleads_clean_field(table_name, column_name) -%}

    {# /* Apply to all tables */ #}

    {%- if column_name == 'customer_id' -%}
        {{column_name}} as account_id

    {%- elif column_name == 'customer_currency_code' -%}
        {{column_name}} as account_currency_code

    {%- elif column_name in ("customer_descriptive_name","descriptive_name") -%}
        {{column_name}} as account_name

    {%- elif column_name == 'ad_group_ad_status' -%}
        {{column_name}} as ad_status

    {%- elif column_name == 'cost_micros' -%}
       {{column_name}}::float/1000000 as spend

    {%- elif column_name == 'campaign_budget_amount_micros' -%}
       {{column_name}}::float/1000000 as campaign_budget_amount
    
    {#- /*  End  */ -#}

    {#- /* Apply to specific table */ -#}
    {%- elif "account" in table_name -%}

        {%- if column_name in ("id","currency_code") -%}
        {{column_name}} as account_{{column_name}}

        {%- else -%}
        {{column_name}}
        
        {%- endif -%}

    {%- elif "campaign" in table_name -%}

        {%- if column_name in ("id","name","status") -%}
        {{column_name}} as campaign_{{column_name}}

        {%- else -%}
        {{column_name}}
        
        {%- endif -%}

    {%- elif "ad_group" in table_name -%}

        {%- if column_name in ("id","name","status") -%}
        {{column_name}} as ad_group_{{column_name}}

        {%- else -%}
        {{column_name}}
        
        {%- endif -%}

    {%- elif "ad" in table_name -%}

        {%- if column_name in ("id","name","status","final_urls") -%}
        {{column_name}} as ad_{{column_name}}

        {%- elif table_name == "expanded_text_ads" and "headline_part" in column_name -%}
        {{column_name}} as expanded_text_ad_{{column_name}}

        {%- else -%}
        {{column_name}}
        
        {%- endif -%}

    {%- elif "keyword" in table_name -%}

        {%- if column_name in ("id","ad_group_criterion_criterion_id","keyword_ad_group_criterion") -%}
        {{column_name}} as keyword_id

        {%- elif column_name == 'cpc_bid_micros' -%}
        {{column_name}}::float/1000000 as keyword_cpc_bid

        {%- elif column_name in ("status","ad_group_criterion_status") -%}
        {{column_name}} as keyword_status

        {%- elif column_name == "negative" -%}
        {{column_name}} as keyword_{{column_name}}

        {%- elif column_name == "info_text" -%}
        {{column_name}} as keyword_text

        {%- elif column_name == "info_match_type" -%}
        {{column_name}} as keyword_match_type

        {%- else -%}
        {{column_name}}
        
        {%- endif -%}

    {%- elif 'search_term_performance' in table_name -%}

        {%- if column_name == 'status' -%}
        {{column_name}} as search_term_status

        {%- else -%}
        {{column_name}}
        
        {%- endif -%}

    {%- elif 'asset_group' in table_name -%}

        {%- if column_name == 'asset_group_id' -%}
        {{column_name}}::VARCHAR as asset_group_id

        {%- elif column_name == 'campaign_id' -%}
        {{column_name}}::VARCHAR as campaign_id

        {%- elif column_name == 'customer_id' -%}
        {{column_name}}::VARCHAR as customer_id

        {%- else -%}
        {{column_name}}
        
        {%- endif -%}


    {%- else -%}
    {{column_name}}
        
    {%- endif -%}

{% endmacro -%}
