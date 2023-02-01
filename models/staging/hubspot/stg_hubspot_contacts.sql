WITH source AS (
    SELECT * 
    FROM {{ source('hubspot', 'contacts')}}
),

contacts AS (
    SELECT *
    FROM source
),

transformed AS (
    SELECT CONCAT('hubspot-', id) as id
    , first_name
    , last_name
    , REPLACE(TRANSLATE(phone, '(,),-,.', ''), ' ', '') AS new_phone
    , CASE WHEN LEN(new_phone)=10 THEN 
        '(' || SUBSTR(new_phone, 1, 3) ||
        ') ' || SUBSTR(new_phone, 4, 3) ||
        '-' || SUBSTR(new_phone, 7, 4) 
        END AS phone    
    , business_name
    FROM contacts
)

SELECT * FROM transformed