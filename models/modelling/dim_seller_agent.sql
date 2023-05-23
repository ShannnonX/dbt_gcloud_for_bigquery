{{
    config(
        materialized = 'view'
    )
}}

with stg_seller_agent as (
    SELECT DISTINCT sellerG
    FROM {{ref('silver_full_column_version_updated_total')}}
    ORDER BY sellerG ASC
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['stg_seller_agent.sellerG']) }} as SellerAgentKey, 
  sellerG as SellerAgent
FROM stg_seller_agent