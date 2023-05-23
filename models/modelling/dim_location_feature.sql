{{
    config(
        materialized = 'view'
    )
}}

with stg_location_feature as (
    SELECT
        Regionname,
        CouncilArea,
        Suburb,
        Postcode,
        Propertycount
    FROM
        {{ref('silver_full_column_version_updated_total')}}
    GROUP BY
        Regionname,
        CouncilArea, 
        Suburb,
        Postcode, 
        Propertycount
    ORDER BY
        Regionname, CouncilArea, Suburb ASC
)

SELECT
    {{ dbt_utils.generate_surrogate_key(['stg_location_feature.Regionname', 'CouncilArea', 'Suburb', 'Postcode', 'Propertycount']) }} as Id,
     Regionname,
     CouncilArea,
     Suburb,
     Postcode, 
     Propertycount
FROM stg_location_feature
