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

clean_duplicate_row as (
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
            all_data ) 
        WHERE
        row_number = 1 OR ( row_number > 1 AND  DATE_DIFF(date, previous_date, DAY) > 45) 
        order by Date ASC
),

date_ranges AS (
  SELECT 
    MAX(date) AS latest_date,
    DATE_SUB(MAX(date), INTERVAL 30 DAY) AS test_start_date,
    DATE_SUB(MAX(date), INTERVAL 90 DAY) AS validate_start_date
  FROM clean_duplicate_row
)

SELECT
  * EXCEPT(latest_date, test_start_date, validate_start_date, Regionname, row_number, previous_date),
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
  clean_duplicate_row, date_ranges
WHERE landsize < 2000 and price < 5000000.0 and price > 100000
