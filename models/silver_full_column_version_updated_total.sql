{{
    config(
        materialized = 'table'
    )
}}

WITH all_data AS (
    SELECT *
    FROM  {{ ref('silver_full_column_version_updated')  }}
    UNION ALL
    SELECT *
    FROM  {{ ref('silver_full_column_version_updated_test')  }}
),

date_ranges AS (
  SELECT 
    MAX(date) AS latest_date,
    DATE_SUB(MAX(date), INTERVAL 30 DAY) AS test_start_date,
    DATE_SUB(MAX(date), INTERVAL 90 DAY) AS validate_start_date
  FROM all_data
)

SELECT
  * EXCEPT(latest_date, test_start_date, validate_start_date, Regionname),
  CASE
    WHEN date >= test_start_date THEN 'Test'
    WHEN (date >= validate_start_date and date < test_start_date) THEN 'Validate'
    WHEN date < validate_start_date THEN 'Train'
  END AS dataType, 
  CASE
      WHEN Regionname = 'Eastern Victoria' THEN 'Eastern Metropolitan'
      WHEN Regionname = 'Western Victoria' THEN 'Western Metropolitan'
      WHEN Regionname = 'Northern Victoria' THEN 'Northern Metropolitan'
      ELSE Regionname
  END AS Regionname
FROM
  all_data, date_ranges
WHERE landsize < 2000 and price < 9000000.0
