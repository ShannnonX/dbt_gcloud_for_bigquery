{{
    config(
        materialized = 'view'
    )
}}

with stg_property_type as (
    SELECT DISTINCT Type
    FROM {{ref('silver_full_column_version_updated_total')}}
    ORDER BY Type ASC
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['stg_property_type.Type']) }} as PropertyTypeKey, 
  CASE Type
    WHEN 'u' THEN 'Unit'
    WHEN 'h' THEN 'House'
    WHEN 't' THEN 'Townhouse'
    ELSE Type
  END AS PropertyType
FROM stg_property_type                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       