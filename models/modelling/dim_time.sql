{{
    config(
        materialized = 'view'
    )
}}

with stg_time as (
    SELECT DISTINCT Date
    FROM {{ref('silver_full_column_version_updated')}}
    ORDER BY Date ASC
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['stg_time.Date']) }} as TimeKey, 
  Date,
  EXTRACT(YEAR FROM Date) as Year, 
  EXTRACT(MONTH FROM  Date) as Month,
  FORMAT_DATE('%Y-%m', Date) as YearMonth,
  EXTRACT(QUARTER FROM Date) as Quarter
FROM stg_time