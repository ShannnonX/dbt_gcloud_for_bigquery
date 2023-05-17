{{
    config(
        materialized = 'view'
    )
}}

with stg_property as (
    select
        *
    from {{ ref('silver_full_column_version_updated')  }}
)

select
    {{ dbt_utils.generate_surrogate_key(['stg_property.address', 'suburb']) }} as PropertyKey,
    {{ dbt_utils.generate_surrogate_key(['Date']) }} as PropertyTimeKey,
    stg_property.Date,
    stg_property.Price,
    stg_property.Rooms,
    stg_property.Bathroom,
    stg_property.Car
from stg_property
ORDER by Date ASC
