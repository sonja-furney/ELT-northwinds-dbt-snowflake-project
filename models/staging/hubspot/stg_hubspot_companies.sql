WITH source AS (
    SELECT * 
    FROM {{ source('hubspot', 'companies')}}
),

companies AS (
    SELECT *
    FROM source
),

transformed AS (
    SELECT CONCAT('hubspot-', id) as company_id
    , first_name
    , last_name
    , REPLACE(TRANSLATE(phone, '(,),-,.', ''), ' ', '') AS new_phone
    , CASE WHEN LEN(new_phone)=10 THEN 
        '(' || SUBSTR(new_phone, 1, 3) ||
        ') ' || SUBSTR(new_phone, 4, 3) ||
        '-' || SUBSTR(new_phone, 7, 4) 
        END AS phone    
    , business_name
    FROM companies

)

SELECT company_id
    , business_name 
FROM transformed
GROUP BY business_name