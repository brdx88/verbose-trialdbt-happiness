SELECT
    payment_id,
    loan_id,
    payment_date,
    payment_amount,
    principal_component,
    interest_component,
    penalty_component,
    payment_method,
    payment_status
FROM {{ source('banking_bronze', 'payments') }}
WHERE _PARTITIONDATE = '2025-10-02'