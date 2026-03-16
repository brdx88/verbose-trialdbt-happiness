WITH payments AS (
    SELECT
        CAST(loan_id as STRING) as loan_id,
        CAST(payment_method as STRING) as payment_method,
        CAST(payment_amount as FLOAT64) as payment_amount,
        CAST(payment_date as DATE) as payment_date,
        CAST(payment_status as STRING) as payment_status,
        CAST(payment_id as STRING) as payment_id
    FROM {{ source('banking_bronze', 'risk_payments_raw') }}
)

SELECT * FROM payments
