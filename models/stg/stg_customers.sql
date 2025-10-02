WITH src AS 
(
    SELECT *
    FROM {{ source('bronze', 'customers') }}
    WHERE _PARTITIONDATE = '2025-10-01'
)

SELECT
    customer_id,
    full_name,
    date_of_birth,
    gender,
    email,
    phone_number,
    address,
    city, province,
    country,
    join_date,
    customer_segment,
    kyc_status
FROM src