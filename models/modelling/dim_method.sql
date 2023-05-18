{{
    config(
        materialized = 'view'
    )
}}

with stg_method as (
    SELECT DISTINCT Method
    FROM {{ref('silver_full_column_version_updated_total')}}
    ORDER BY Method ASC
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['stg_method.Method']) }} as MethodKey, 
  Method
FROM stg_method 