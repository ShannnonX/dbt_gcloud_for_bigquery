{{
    config(
        materialized = 'view'
    )
}}

SELECT
  'Suburb' AS column_name, COUNTIF(Suburb IS NULL) AS Null_Count
  FROM `house-prediction-381923.melbourne_house_data.bronze_full_column_version_updated` UNION ALL
  SELECT 'Address' AS column_name, COUNTIF(Address IS NULL) AS null_count FROM `house-prediction-381923.melbourne_house_data.bronze_full_column_version_updated` UNION ALL
  SELECT 'Rooms' AS column_name, COUNTIF(Rooms IS NULL) AS null_count FROM `house-prediction-381923.melbourne_house_data.bronze_full_column_version_updated` UNION ALL
  SELECT 'Type' AS column_name, COUNTIF(Type IS NULL) AS null_count FROM `house-prediction-381923.melbourne_house_data.bronze_full_column_version_updated` UNION ALL
  SELECT 'Price' AS column_name, COUNTIF(Price IS NULL) AS null_count FROM `house-prediction-381923.melbourne_house_data.bronze_full_column_version_updated` UNION ALL
  SELECT 'Method' AS column_name, COUNTIF(Method IS NULL) AS null_count FROM `house-prediction-381923.melbourne_house_data.bronze_full_column_version_updated` UNION ALL
  SELECT 'SellerG' AS column_name, COUNTIF(SellerG IS NULL) AS null_count FROM `house-prediction-381923.melbourne_house_data.bronze_full_column_version_updated` UNION ALL
  SELECT 'Date' AS column_name, COUNTIF(Date IS NULL) AS null_count FROM `house-prediction-381923.melbourne_house_data.bronze_full_column_version_updated` UNION ALL
  SELECT 'Distance' AS column_name, COUNTIF(Distance IS NULL) AS null_count FROM `house-prediction-381923.melbourne_house_data.bronze_full_column_version_updated` UNION ALL
  SELECT 'Postcode' AS column_name, COUNTIF(Postcode IS NULL) AS null_count FROM `house-prediction-381923.melbourne_house_data.bronze_full_column_version_updated` UNION ALL
  SELECT 'Bedroom2' AS column_name, COUNTIF(Bedroom2 IS NULL) AS null_count FROM `house-prediction-381923.melbourne_house_data.bronze_full_column_version_updated`UNION ALL
  SELECT 'Bathroom' AS column_name, COUNTIF(Bathroom IS NULL) AS null_count FROM `house-prediction-381923.melbourne_house_data.bronze_full_column_version_updated` UNION ALL
  SELECT 'Car' AS column_name, COUNTIF(Car IS NULL) AS null_count FROM `house-prediction-381923.melbourne_house_data.bronze_full_column_version_updated` UNION ALL
  SELECT 'Landsize' AS column_name, COUNTIF(Landsize IS NULL) AS null_count FROM `house-prediction-381923.melbourne_house_data.bronze_full_column_version_updated` UNION ALL
  SELECT 'BuildingArea' AS column_name, COUNTIF(BuildingArea IS NULL) AS null_count FROM `house-prediction-381923.melbourne_house_data.bronze_full_column_version_updated` UNION ALL
  SELECT 'YearBuilt' AS column_name, COUNTIF(YearBuilt IS NULL) AS null_count FROM `house-prediction-381923.melbourne_house_data.bronze_full_column_version_updated` UNION ALL
  SELECT 'CouncilArea' AS column_name, COUNTIF(CouncilArea IS NULL) AS null_count FROM `house-prediction-381923.melbourne_house_data.bronze_full_column_version_updated` UNION ALL
  SELECT 'Lattitude' AS column_name, COUNTIF(Lattitude IS NULL) AS null_count FROM `house-prediction-381923.melbourne_house_data.bronze_full_column_version_updated` UNION ALL
  SELECT 'Longtitude' AS column_name, COUNTIF(Longtitude IS NULL) AS null_count FROM `house-prediction-381923.melbourne_house_data.bronze_full_column_version_updated` UNION ALL
  SELECT 'Regionname' AS column_name, COUNTIF(Regionname IS NULL) AS null_count FROM `house-prediction-381923.melbourne_house_data.bronze_full_column_version_updated` UNION ALL
  SELECT 'Propertycount' AS column_name, COUNTIF(Propertycount IS NULL) AS null_count FROM `house-prediction-381923.melbourne_house_data.bronze_full_column_version_updated`

ORDER BY
  Null_Count DESC