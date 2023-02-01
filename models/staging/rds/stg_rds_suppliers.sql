WITH source AS (
    SELECT * 
    FROM {{ source('rds', 'suppliers')}}
),

renamed AS (
    SELECT 
    replace(replace(replace(replace(replace(phone, '-', ''), ')', ''), '(', ''), '.', ''), ' ', '') AS new_phone
    , CASE WHEN LEN(new_phone)=10 THEN new_phone ELSE null END AS changed_phone
    , CONCAT(
    '(', SUBSTR(changed_phone, 1, 3),
    ') ', SUBSTR(changed_phone, 4, 3),
    '-', SUBSTR(changed_phone, 7, 4)) as phone
    , SPLIT_PART(contact_name, ' ', 1) AS first_name
    , SPLIT_PART(contact_name, ' ', -1) AS last_name
    , PHONE AS old_phone
    , SUPPLIER_ID
    , COMPANY_NAME
    , CONTACT_NAME
    , CONTACT_TITLE
    , ADDRESS
    , CITY
    , REGION
    , POSTAL_CODE
    , COUNTRY
    , FAX
    , HOMEPAGE
    FROM source
)

select 
    phone
    , SUPPLIER_ID
    , COMPANY_NAME
    , first_name
    , last_name
    , CONTACT_TITLE
    , ADDRESS
    , CITY
    , REGION
    , POSTAL_CODE
    , COUNTRY
    , FAX
    , HOMEPAGE
FROM renamed
