SELECT
    CAST(loan_id AS INTEGER) AS loan_id,
    CAST(customer_id AS INTEGER) AS customer_id,
    CAST(UPPER(loan_type) AS STRING) AS loan_type,
    CAST(principal_amount AS INTEGER) AS principal_amount,
    CAST(interest_rate AS FLOAT64) AS interest_rate,
    CAST(tenor_months AS INTEGER) AS tenor_months,
    CAST(disbursement_date AS DATE) AS disbursement_date,
    CAST(maturity_date AS DATE) AS maturity_date,
    CAST(outstanding_balance AS INTEGER) AS outstanding_balance,
    CAST(UPPER(loan_status) AS STRING) AS loan_status
FROM {{ source('banking_bronze', 'loans') }}
WHERE _PARTITIONDATE = '2025-10-02'