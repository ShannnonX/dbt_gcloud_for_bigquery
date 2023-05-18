{{
    config(
        materialized = 'view'
    )
}}

with stg_suburb as (
    SELECT DISTINCT Suburb, Propertycount
    FROM {{ref('silver_full_column_version_updated_total')}}
    ORDER BY Suburb, Propertycount ASC
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['stg_suburb.Suburb']) }} as SuburbKey, 
  Suburb,
  Propertycount
FROM stg_suburb

