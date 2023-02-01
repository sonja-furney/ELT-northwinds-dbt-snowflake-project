WITH source AS (
    SELECT * 
    FROM {{source('rds', 'customers')}}
),

companies AS (
    SELECT *
    FROM stg_rds_companies
),

renamed AS (
        SELECT concat('rds-',replace(lower(company_name), ' ', '-')) as company_id
        , company_name
        , MAX(ADDRESS) AS ADDRESS
        , MAX(CITY) AS CITY
        , MAX(POSTAL_CODE) AS POSTAL_CODE
        , MAX(COUNTRY) AS COUNTRY
        FROM source
        GROUP BY COMPANY_NAME
)
SELECT * 
FROM renamed