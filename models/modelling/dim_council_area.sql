{{
    config(
        materialized = 'view'
    )
}}

with stg_council_area as (
    SELECT DISTINCT CouncilArea
    FROM {{ref('silver_full_column_version_updated_total')}}
    ORDER BY CouncilArea ASC
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['stg_council_area.CouncilArea']) }} as CouncilAreaKey, 
  CouncilArea
FROM stg_council_area 