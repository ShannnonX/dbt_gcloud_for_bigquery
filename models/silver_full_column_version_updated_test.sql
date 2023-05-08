{{
    config(
        materialized = 'table'
    )
}}


SELECT
  Suburb,
  Address,
  Rooms,
  Type,
  Price,
  Method,
  SellerG,
  DATE(PARSE_DATE('%Y-%m-%d', Date)) AS Date,
  Distance,
  CAST(SAFE_CAST(Postcode AS INT64) AS STRING) AS Postcode,
  SAFE_CAST(Bathroom AS INT64) AS Bathroom,
  SAFE_CAST(Car AS INT64) AS Car,
  Landsize,
  CouncilArea,
  Regionname,
  SAFE_CAST(Propertycount AS INT64) AS Propertycount,
FROM
  `house-prediction-381923.melbourne_house_data.bronze_full_column_version_updated_test`
WHERE
  Price IS NOT NULL
  AND suburb IS NOT NULL
  AND Address IS NOT NULL
  AND rooms IS NOT NULL
  AND type IS NOT NULL
  AND method IS NOT NULL
  AND sellerG IS NOT NULL
  AND Date IS NOT NULL
  AND Distance IS NOT NULL
  AND Postcode IS NOT NULL
  AND Bathroom IS NOT NULL
  AND Car IS NOT NULL
  AND Landsize IS NOT NULL
  AND CouncilArea IS NOT NULL
  AND Regionname IS NOT NULL
  AND Propertycount IS NOT NULL 