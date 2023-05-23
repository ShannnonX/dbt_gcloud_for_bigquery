{{
    config(
        materialized = 'view'
    )
}}

with stg_property as (
    SELECT *
    FROM  {{ ref('silver_full_column_version_updated_total')  }}
)

select
    {{ dbt_utils.generate_surrogate_key(['stg_property.Address', 'Suburb']) }} as PropertyKey,
    {{ dbt_utils.generate_surrogate_key(['stg_property.Regionname', 'CouncilArea', 'Suburb', 'Postcode', 'Propertycount']) }} as PropertyLocationKey,
    {{ dbt_utils.generate_surrogate_key(['Date']) }} as PropertyDateKey,
    {{ dbt_utils.generate_surrogate_key(['Method']) }} as PropertySellingMethodKey,
    {{ dbt_utils.generate_surrogate_key(['SellerG']) }} as PropertySellerAgentKey,
    {{ dbt_utils.generate_surrogate_key(['Type']) }} as PropertyTypeKey,
    stg_property.Price,
    stg_property.Rooms,
    stg_property.Bathroom,
    stg_property.Car,
    stg_property.Landsize
from stg_property
ORDER by Date ASC