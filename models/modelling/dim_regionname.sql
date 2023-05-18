{{
    config(
        materialized = 'view'
    )
}}

with stg_regionname as (
    SELECT DISTINCT Regionname
    FROM {{ref('silver_full_column_version_updated_total')}}
    ORDER BY Regionname ASC
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['stg_regionname.Regionname']) }} as RegionnameKey, 
  Regionname,
FROM stg_regionname