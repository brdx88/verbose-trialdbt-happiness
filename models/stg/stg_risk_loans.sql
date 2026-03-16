WITH loans AS (
    SELECT
        CAST(loan_id as STRING) as loan_id,
        CAST(customer_id as STRING) as customer_id,
        CAST(loan_amount as FLOAT64) as loan_amount,
        CAST(interest_rate as FLOAT64) as interest_rate,
        CAST(start_date as DATE) as loan_date,
        CAST(loan_status as STRING) as loan_status
    FROM {{ source('banking_bronze', 'risk_loans_raw') }}
)

SELECT * FROM loans
