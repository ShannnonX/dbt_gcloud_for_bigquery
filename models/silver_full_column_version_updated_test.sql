{{
    config(
        materialized = 'table'
    )
}}

with clean_duplicate_row as (
    SELECT
        *
        FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY suburb, address, CAST(price AS STRING)
            ORDER BY
            date) AS row_number,
            LAG(date) OVER (PARTITION BY suburb, address, CAST(price AS STRING)
            ORDER BY
            date) AS previous_date
        FROM
            `house-prediction-381923.melbourne_house_data.bronze_full_column_version_updated_test` ) 
        WHERE
        row_number = 1 OR ( row_number > 1 AND  DATE_DIFF(PARSE_DATE('%Y-%m-%d', date), PARSE_DATE('%Y-%m-%d', previous_date), DAY) > 45) 
)

SELECT
  Bronze.Suburb,
  Bronze.Address,
  Bronze.Rooms,
  Bronze.Type,
  Bronze.Price,
  Bronze.Method,
  Bronze.SellerG,
  DATE(PARSE_DATE('%Y-%m-%d', Bronze.Date)) AS Date,
  Bronze.Distance,
  CAST(SAFE_CAST(Bronze.Postcode AS INT64) AS STRING) AS Postcode,
  SAFE_CAST(Bronze.Bathroom AS INT64) AS Bathroom,
  SAFE_CAST(Bronze.Car AS INT64) AS Car,
  Bronze.Landsize,
  Bronze.CouncilArea,
  Bronze.Regionname,
  SAFE_CAST(Bronze.Propertycount AS INT64) AS Propertycount,
  FORMAT_DATE('%Y-%m', PARSE_DATE('%Y-%m-%d', bronze.Date)) AS YearMonth,
  Residential.IndexPrice
FROM
  clean_duplicate_row Bronze
LEFT JOIN
  `house-prediction-381923.melbourne_house_data.MonthlyResidentialPropertyPrice` AS Residential
ON
  FORMAT_DATE('%Y-%m', PARSE_DATE('%Y-%m-%d', Bronze.Date)) = Residential.YearMonth
WHERE
  Bronze.Price IS NOT NULL
  AND Bronze.suburb IS NOT NULL
  AND Bronze.Address IS NOT NULL
  AND Bronze.Rooms IS NOT NULL
  AND Bronze.Type IS NOT NULL
  AND Bronze.method IS NOT NULL
  AND Bronze.sellerG IS NOT NULL
  AND Bronze.Date IS NOT NULL
  AND Bronze.Distance IS NOT NULL
  AND Bronze.Postcode IS NOT NULL
  AND Bronze.Bathroom IS NOT NULL
  AND Bronze.Car IS NOT NULL
  AND Bronze.Landsize IS NOT NULL
  AND Bronze.CouncilArea IS NOT NULL
  AND Bronze.Regionname IS NOT NULL
  AND Bronze.Propertycount IS NOT NULL 
ORDER BY
  Bronze.Date ASC