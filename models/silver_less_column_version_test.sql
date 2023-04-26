{{
    config(
        materialized = 'table'
    )
}}


SELECT * FROM `house-prediction-381923.melbourne_house_data.bronze_less_column_version_test`   WHERE Price IS NOT NULL 
