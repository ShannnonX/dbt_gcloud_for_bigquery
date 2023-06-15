{{
    config(
        materialized = 'table'
    )
}}


WITH cleansing_table as (
    SELECT
        *EXCEPT( Regionname, CouncilArea),
        IF(CouncilArea='#N/A', NULL, SAFE_CAST(CouncilArea AS STRING)) AS CouncilArea,
        CASE
            WHEN Regionname = 'Eastern Victoria' THEN 'Eastern Metropolitan'
            WHEN Regionname = 'Western Victoria' THEN 'Western Metropolitan'
            WHEN Regionname = 'Northern Victoria' THEN 'Northern Metropolitan'
            WHEN Regionname = '#N/A' THEN ''
            ELSE Regionname
        END AS Regionname,
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
            `house-prediction-381923.melbourne_house_data.bronze_full_column_version_updated2` )
        WHERE
        row_number = 1 OR ( row_number > 1 AND  DATE_DIFF(Date,  previous_date, DAY) > 45)
),

date_ranges AS (
  SELECT
    MAX(date) AS latest_date,
    DATE_SUB(MAX(date), INTERVAL 30 DAY) AS test_start_date,
    DATE_SUB(MAX(date), INTERVAL 90 DAY) AS validate_start_date
  FROM cleansing_table
),

add_dataType AS (
  SELECT
    * EXCEPT(latest_date, test_start_date, validate_start_date, row_number, previous_date),
    CASE
        WHEN date >= test_start_date THEN 'Test'
        WHEN (date >= validate_start_date and date < test_start_date) THEN 'Validate'
        WHEN date < validate_start_date THEN 'Train'
    END AS dataType,
    FROM
    cleansing_table, date_ranges
    WHERE  landsize < 2000 and price < 5000000.0 and price > 100000
  )

SELECT
  Bronze.*EXCEPT(Postcode, Bathroom, Car,Propertycount, Bedroom2, BuildingArea, YearBuilt, Lattitude, Longtitude),
  CAST(SAFE_CAST(Bronze.Postcode AS INT64) AS STRING) AS Postcode,
  SAFE_CAST(Bronze.Bathroom AS INT64) AS Bathroom,
  SAFE_CAST(Bronze.Car AS INT64) AS Car,
  SAFE_CAST(Bronze.Propertycount AS INT64) AS Propertycount,
  COALESCE(PreAbsIndexPrice2.YearMonth, PreAbsIndexPrice.YearMonth) AS ABSYearMonth,
  COALESCE(PreAbsIndexPrice2.melbourne, PreAbsIndexPrice.melbourne) AS IndexPriceAbs
FROM
  add_dataType Bronze
LEFT JOIN
  `house-prediction-381923.melbourne_house_data.ResidentialPropertyPrice` AS AbsIndexPrice
ON
  FORMAT_DATE('%Y-%m', Bronze.Date) = AbsIndexPrice.YearMonth
LEFT JOIN
  `house-prediction-381923.melbourne_house_data.ResidentialPropertyPrice` AS PreAbsIndexPrice
ON
  AbsIndexPrice.YearMonth IS NULL
  AND FORMAT_DATE('%Y-%m', Bronze.Date) > PreAbsIndexPrice.YearMonth
  AND (
    PreAbsIndexPrice.YearMonth = FORMAT_DATE('%Y-%m', DATE_SUB(Bronze.Date, INTERVAL 1 MONTH))
    OR PreAbsIndexPrice.YearMonth = FORMAT_DATE('%Y-%m', DATE_SUB(Bronze.Date, INTERVAL 2 MONTH))
    )
 LEFT JOIN
  `house-prediction-381923.melbourne_house_data.ResidentialPropertyPrice` AS PreAbsIndexPrice2
ON
  AbsIndexPrice.YearMonth IS NOT NULL
  AND (
    PreAbsIndexPrice2.YearMonth = FORMAT_DATE('%Y-%m', DATE_SUB(Bronze.Date, INTERVAL 3 MONTH))
    )
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

