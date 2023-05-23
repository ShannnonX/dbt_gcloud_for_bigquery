{{
    config(
        materialized = 'view'
    )
}}

with stg_date as (
    SELECT DISTINCT Date
    FROM {{ref('silver_full_column_version_updated_total')}}
    ORDER BY Date ASC
)

SELECT
  {{ dbt_utils.generate_surrogate_key(['stg_date.Date']) }} as DateKey, 
  Date,
  EXTRACT(DAYOFWEEK FROM Date) AS DayOfWeek,
  CASE EXTRACT(DAYOFWEEK FROM Date)
          WHEN 1 THEN 'Sunday'
          WHEN 2 THEN 'Monday'
          WHEN 3 THEN 'Tuesday'
          WHEN 4 THEN 'Wednesday'
          WHEN 5 THEN 'Thursday'
          WHEN 6 THEN 'Friday'
          WHEN 7 THEN 'Saturday'
        END AS DayOfWeekName,
  FLOOR((EXTRACT(DAY FROM Date) - 1) / 7) + 1 AS WeekOfMonth,
  EXTRACT(WEEK FROM Date) AS WeekOfYear,
  EXTRACT(MONTH FROM  Date) as Month,
  CASE EXTRACT(MONTH FROM Date)
          WHEN 1 THEN 'January'
          WHEN 2 THEN 'February'
          WHEN 3 THEN 'March'
          WHEN 4 THEN 'April'
          WHEN 5 THEN 'May'
          WHEN 6 THEN 'June'
          WHEN 7 THEN 'July'
          WHEN 8 THEN 'August'
          WHEN 9 THEN 'September'
          WHEN 10 THEN 'October'
          WHEN 11 THEN 'November'
          WHEN 12 THEN 'December'
        END AS MonthName,
  LEFT(CASE EXTRACT(MONTH FROM Date)
          WHEN 1 THEN 'Jan'
          WHEN 2 THEN 'Feb'
          WHEN 3 THEN 'Mar'
          WHEN 4 THEN 'Apr'
          WHEN 5 THEN 'May'
          WHEN 6 THEN 'Jun'
          WHEN 7 THEN 'Jul'
          WHEN 8 THEN 'Aug'
          WHEN 9 THEN 'Sep'
          WHEN 10 THEN 'Oct'
          WHEN 11 THEN 'Nov'
          WHEN 12 THEN 'Dec'
     END, 3) AS MonthNameShort,
  EXTRACT(YEAR FROM Date) as Year, 
  EXTRACT(QUARTER FROM Date) as Quarter, 
  CASE EXTRACT(QUARTER FROM Date)
          WHEN 1 THEN 'Q1'
          WHEN 2 THEN 'Q2'
          WHEN 3 THEN 'Q3'
          WHEN 4 THEN 'Q4'
        END AS QuarterName,
  FORMAT_DATE('%Y-%m', Date) as YearMonth,
  FORMAT_DATE('%m%Y', Date) as MmYear,
FROM stg_date                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         