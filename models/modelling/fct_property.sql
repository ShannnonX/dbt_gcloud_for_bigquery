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
    {{ dbt_utils.generate_surrogate_key(['stg_property.address', 'suburb']) }} as PropertyKey,
    {{ dbt_utils.generate_surrogate_key(['Date']) }} as PropertyTimeKey,
    {{ dbt_utils.generate_surrogate_key(['Suburb']) }} as PropertySuburbKey,
    {{ dbt_utils.generate_surrogate_key(['Method']) }} as PropertyMethodKey,
    {{ dbt_utils.generate_surrogate_key(['Regionname']) }} as PropertyRegionnameKey,
    {{ dbt_utils.generate_surrogate_key(['sellerG']) }} as PropertySellerGKey,
    {{ dbt_utils.generate_surrogate_key(['Type']) }} as PropertyTypeKey,
    {{ dbt_utils.generate_surrogate_key(['CouncilArea']) }} as PropertyCouncilAreaKey,
    stg_property.Suburb,
    stg_property.Date,
    stg_property.Price,
    stg_property.Rooms,
    stg_property.Bathroom,
    stg_property.Car
from stg_property
ORDER by Date ASC