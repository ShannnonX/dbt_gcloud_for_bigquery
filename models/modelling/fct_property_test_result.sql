{{
    config(
        materialized = 'view'
    )
}}

with stg_property_test_result as (
    SELECT *
    FROM  `house-prediction-381923.melbourne_house_data.monitoring_report_raw_new_test_set` 
)

select
    {{ dbt_utils.generate_surrogate_key(['stg_property_test_result.Regionname']) }} as PropertyTestResultLocationKey,
    {{ dbt_utils.generate_surrogate_key(['Type']) }} as PropertyTestResultTypeKey,
    stg_property_test_result.Rooms,
    stg_property_test_result.Bathroom,
    stg_property_test_result.Car,
    stg_property_test_result.Landsize,
    stg_property_test_result.target,
    stg_property_test_result.prediction,
    stg_property_test_result.Regionname
from stg_property_test_result