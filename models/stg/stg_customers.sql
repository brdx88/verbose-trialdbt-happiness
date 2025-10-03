SELECT
    CAST(customer_id AS INTEGER) AS customer_id,
    CAST(UPPER(full_name) AS STRING) AS full_name,
    CAST(date_of_birth AS DATE) AS date_of_birth,

    CAST(CASE 
        WHEN gender = 'M' THEN 'MALE'
        WHEN gender = 'F' THEN 'FEMALE'
    END AS STRING) as gender,

    CAST(LOWER(email) AS STRING) AS email,
    CAST(phone_number AS STRING) AS phone_number,
    CAST(UPPER(address) AS STRING) AS address,
    CAST(UPPER(city) AS STRING) AS city, province,
    CAST(UPPER(country) AS STRING) AS country,
    CAST(join_date AS DATE) AS join_date,
    CAST(UPPER(customer_segment) AS STRING) AS customer_segment,
    CAST(UPPER(kyc_status) AS STRING) AS kyc_status
FROM {{ source('banking_bronze', 'customers') }}
WHERE _PARTITIONDATE = '2025-10-02'