{%- set ad_selected_fields = [
    "ad_group_id",
    "id",
    "name",
    "status",
    "final_urls",
    "updated_at"
] -%}

{%- set expanded_text_ad_selected_fields = [
    "ad_group_id",
    "ad_id",
    "headline_part_1",
    "headline_part_2",
    "headline_part_3",
    "updated_at"
] -%}

{%- set expanded_dynamic_search_ad_selected_fields = [
    "ad_group_id",
    "ad_id",
    "description"
] -%}

{%- set gmail_ad_selected_fields = [
    "ad_group_id",
    "ad_id",
    "teaser_headline"
] -%}

{%- set schema_name, 
        ad_table_name,
        expanded_text_ad_table_name,
        expanded_dynamic_search_ad_table_name,
        gmail_ad_table_name = 
        'googleads_raw', 
        'ads',
        'expanded_text_ads',
        'expanded_dynamic_search_ads',
        'gmail_ads' -%}

WITH ads AS 
    (SELECT
    
        {% for field in ad_selected_fields -%}
        {{ get_googleads_clean_field(ad_table_name, field) }},
        {% endfor -%}
        MAX(updated_at) OVER (PARTITION BY ad_group_id, id) as last_updated_at

    FROM {{ source(schema_name, ad_table_name) }}
    ),

    expanded_text_ads AS 
    (SELECT
    
        {% for field in expanded_text_ad_selected_fields -%}
        {{ get_googleads_clean_field(expanded_text_ad_table_name, field) }},
        {% endfor -%}
        MAX(updated_at) OVER (PARTITION BY ad_group_id, ad_id) as last_updated_at

    FROM {{ source(schema_name, expanded_text_ad_table_name) }}
    )

SELECT 
    ad_group_id,
    ad_id, 
    ad_name,
    ad_status,
    ad_final_urls,
    expanded_text_ad_headline_part_1,
    expanded_text_ad_headline_part_2,
    expanded_text_ad_headline_part_3,
    ad_group_id||'_'||ad_id as unique_key
FROM 
    (SELECT *
    FROM ads 
    WHERE updated_at = last_updated_at) 
LEFT JOIN 
    (SELECT * 
    FROM expanded_text_ads
    WHERE updated_at = last_updated_at) USING(ad_group_id, ad_id) 