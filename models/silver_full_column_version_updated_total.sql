{{
    config(
        materialized = 'table'
    )
}}

SELECT *
FROM  {{ ref('silver_full_column_version_updated')  }}
UNION ALL
SELECT *
FROM  {{ ref('silver_full_column_version_updated_test')  }}