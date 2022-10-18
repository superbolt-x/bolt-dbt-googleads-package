{%- macro get_googleads_default_campaign_types(campaign_name) -%}

 CASE 
    WHEN {{ campaign_name }} ~* 'search' AND {{ campaign_name }} ~* 'brand' AND {{ campaign_name }} !~* 'unbranded|nonbrand' THEN 'Campaign Type: Search Branded'
    WHEN {{ campaign_name }} ~* 'search' AND {{ campaign_name }} ~* 'unbranded|nonbrand' THEN 'Campaign Type: Search Nonbrand'
    WHEN {{ campaign_name }} ~* 'search' THEN 'Campaign Type: Search'
    WHEN {{ campaign_name }} ~* 'shopping' AND {{ campaign_name }} ~* 'brand' AND {{ campaign_name }} !~* 'unbranded|nonbrand' THEN 'Campaign Type: Shopping Branded'
    WHEN {{ campaign_name }} ~* 'shopping' AND {{ campaign_name }} ~* 'unbranded|nonbrand' THEN 'Campaign Type: Shopping Nonbrand'
    WHEN {{ campaign_name }} ~* 'shopping' THEN 'Campaign Type: Shopping'
    WHEN {{ campaign_name }} ~* 'display' THEN 'Campaign Type: Display'
    WHEN {{ campaign_name }} ~* 'youtube' THEN 'Campaign Type: Youtube'
    WHEN {{ campaign_name }} ~* 'pmax|performance max' THEN 'Campaign Type: Performance Max'
    ELSE ''
    END AS campaign_type_default

{%- endmacro -%}