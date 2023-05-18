{{
    config(
        materialized = 'view'
    )
}}

with stg_sellerG as (
    SELECT DISTINCT sellerG
    FROM {{ref('silver_full_column_version_updated_total')}}
    ORDER BY sellerG ASC
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['stg_sellerG.sellerG']) }} as SellerGKey, 
  sellerG,
FROM stg_sellerG