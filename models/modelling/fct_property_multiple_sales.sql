{{
    config(
        materialized = 'view'
    )
}}

with stg_property_multiple_sales as (
    SELECT *
        FROM (
        SELECT
            Regionname, CouncilArea, Propertycount, Postcode,
            Address, Suburb, Type,
            FirstDate, FirstPrice, SecondDate, SecondPrice
        FROM (
            SELECT
            Regionname, CouncilArea, Propertycount, Postcode,
            Address, Suburb, Type,
            MIN(CASE WHEN rn = 1 THEN Date END) AS FirstDate,
            MIN(CASE WHEN rn = 1 THEN Price END) AS FirstPrice,
            MIN(CASE WHEN rn = 2 THEN Date END) AS SecondDate,
            MIN(CASE WHEN rn = 2 THEN Price END) AS SecondPrice
            FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY Address, Suburb ORDER BY Date) AS rn,
                COUNT(*) OVER (PARTITION BY Address, Suburb) AS count
            FROM {{ ref('silver_full_column_version_updated_total')  }}
            ) AS subquery
            WHERE count = 2
            GROUP BY Address, Suburb, Regionname, CouncilArea, Propertycount, Postcode, Type
        )
    ) AS outer_query
    WHERE FirstPrice IS NOT NULL AND secondPrice is not null
)

select
    {{ dbt_utils.generate_surrogate_key(['stg_property_multiple_sales.Address', 'Suburb']) }} as PropertyKey,
    {{ dbt_utils.generate_surrogate_key(['stg_property_multiple_sales.Regionname', 'CouncilArea', 'Suburb', 'Postcode', 'Propertycount']) }} as PropertyLocationKey,
    {{ dbt_utils.generate_surrogate_key(['stg_property_multiple_sales.FirstDate']) }} as PropertyMinDateKey,
    {{ dbt_utils.generate_surrogate_key(['stg_property_multiple_sales.SecondDate']) }} as PropertyMaxDateKey,
    {{ dbt_utils.generate_surrogate_key(['stg_property_multiple_sales.Type']) }} as PropertyTypeKey,
    stg_property_multiple_sales.Address,
    stg_property_multiple_sales.FirstPrice,
    stg_property_multiple_sales.SecondPrice
from stg_property_multiple_sales