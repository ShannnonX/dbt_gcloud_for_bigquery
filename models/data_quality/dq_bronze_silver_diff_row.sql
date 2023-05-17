{{
    config(
        materialized = 'view'
    )
}}

SELECT
  Year, 
  Month,
  FORMAT('%d-%02d', Year, Month) AS YearMonth,
  COUNTIF(table_name = 'bronze') AS bronze_row,
  COUNTIF(table_name = 'silver') AS silver_row
FROM (
  SELECT
    'bronze' AS table_name,
    EXTRACT(YEAR FROM PARSE_DATE('%Y-%m-%d', bronze.Date)) as Year, 
    EXTRACT(MONTH FROM PARSE_DATE('%Y-%m-%d', bronze.Date)) as Month,
    bronze.Address
  FROM `house-prediction-381923.melbourne_house_data.bronze_full_column_version_updated` AS bronze
  UNION ALL
  SELECT
    'silver' AS table_name,
    EXTRACT(YEAR FROM silver.Date) as Year, 
    EXTRACT(MONTH FROM  silver.Date) as Month,
    silver.Address
  FROM `house-prediction-381923.melbourne_house_data.silver_full_column_version_updated` as silver
)
GROUP BY Year, Month
order by YearMonth asc 