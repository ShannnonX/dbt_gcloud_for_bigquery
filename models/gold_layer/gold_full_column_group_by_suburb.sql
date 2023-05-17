{{
    config(
        materialized = 'view'
    )
}}

SELECT
  Suburb,
  Postcode,
  ROUND(AVG(price), 2) AS AveragePrice,
  COUNT(*) AS NumberProperty,
  EXTRACT(YEAR FROM Date) AS Year,
  EXTRACT(Month FROM Date) AS Month,
  FORMAT_DATE('%Y-%m', Date) AS YearMonth,
FROM
  {{ref('silver_full_column_version_updated')}}
GROUP BY
  Suburb,
  postcode,
  Year,
  Month,
  YearMonth