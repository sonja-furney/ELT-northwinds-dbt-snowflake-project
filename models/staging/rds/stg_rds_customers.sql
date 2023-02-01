WITH customers as (
    SELECT * FROM {{ source('rds', 'customers')}}
),

companies AS (
    SELECT *
    FROM stg_rds_companies
),

renamed AS (
    SELECT CONCAT('rds-', customer_id) as customer_id
    , SPLIT_PART(contact_name, ' ', 1) AS first_name
    , SPLIT_PART(contact_name, ' ', -1) AS last_name
    , REPLACE(TRANSLATE(phone, '(,),-,.', ''), ' ', '') AS new_phone
    , CASE WHEN LEN(new_phone)=10 THEN 
        '(' || SUBSTR(new_phone, 1, 3) ||
        ') ' || SUBSTR(new_phone, 4, 3) ||
        '-' || SUBSTR(new_phone, 7, 4) 
        END AS phone    
    , company_id
    FROM customers 
    JOIN companies 
    ON companies.company_name = customers.company_name
),

final AS (
    SELECT customer_id
    , first_name
    , last_name
    , phone
    , company_id 
    FROM renamed
)

SELECT * FROM final