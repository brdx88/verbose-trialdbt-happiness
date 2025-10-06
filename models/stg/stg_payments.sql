SELECT
    CAST(payment_id AS ) AS payment_id,
    CAST(loan_id AS ) AS loan_id,
    CAST(payment_date AS ) AS payment_date,
    CAST(payment_amount AS ) AS payment_amount,
    CAST(principal_component AS ) AS principal_component,
    CAST(interest_component AS ) AS interest_component,
    CAST(penalty_component AS ) AS penalty_component,
    CAST(payment_method AS ) AS payment_method,
    CAST(payment_status AS ) AS payment_status
FROM {{ source('banking_bronze', 'payments') }}
WHERE _PARTITIONDATE = '2025-10-02'