{{ config(
    incremental_strategy = 'insert_overwrite',
    partition_by = {
      "field": "position_date",
      "data_type": "date"
    },
    cluster_by = ["customer_id", "loan_id"]
) }}


SELECT
    CURRENT_DATE('Asia/Jakarta') AS position_date,
    -- DATE_ADD(CURRENT_DATE('Asia/Jakarta'), INTERVAL 1 DAY) AS position_date,
    
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
FROM {{ source('demo_banking_stg', 'stg_loans') }}