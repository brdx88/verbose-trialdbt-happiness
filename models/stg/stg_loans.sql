SELECT
    loan_id,
    customer_id,
    loan_type,
    principal_amount,
    interest_rate,
    tenor_months,
    disbursement_date,
    maturity_date,
    outstanding_balance,
    loan_status
FROM {{ source('banking_bronze', 'loans') }}
WHERE _PARTITIONDATE = '2025-10-02'