{{
    config(
        materialized = 'view'
    )
}}

with stg_type as (
    SELECT DISTINCT Type
    FROM {{ref('silver_full_column_version_updated_total')}}
    ORDER BY Type ASC
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['stg_type.Type']) }} as TypeKey, 
  Type,
FROM stg_type                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       