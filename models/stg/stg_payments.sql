SELECT
    CAST(payment_id AS INTEGER) AS payment_id,
    CAST(loan_id AS INTEGER) AS loan_id,
    CAST(payment_date AS DATE) AS payment_date,
    CAST(payment_amount AS INTEGER) AS payment_amount,
    CAST(principal_component AS INTEGER) AS principal_component,
    CAST(interest_component AS INTEGER) AS interest_component,
    CAST(penalty_component AS INTEGER) AS penalty_component,
    CAST(UPPER(payment_method) AS STRING) AS payment_method,
    CAST(UPPER(payment_status) AS STRING) AS payment_status
FROM {{ source('banking_bronze', 'payments') }}
WHERE _PARTITIONDATE = '2025-10-02'