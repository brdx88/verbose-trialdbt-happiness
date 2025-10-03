SELECT 
    CAST(account_id AS INTEGER) AS account_id,
    CAST(customer_id AS INTEGER) AS customer_id,
    CAST(UPPER(account_type) AS STRING) AS account_type,
    CAST(UPPER(currency) AS STRING) AS currency,
    CAST(balance AS INTEGER) AS balance,
    CAST(UPPER(status) AS STRING) AS account_status,
    CAST(open_date AS DATE) AS open_date,
    CAST(close_date AS DATE) AS close_date,
    CAST(branch_id AS INTEGER) AS branch_id,
    CAST(UPPER(risk_score) AS STRING) AS risk_score 
FROM {{ source('banking_bronze', 'accounts') }}
WHERE _PARTITIONDATE = '2025-10-02'