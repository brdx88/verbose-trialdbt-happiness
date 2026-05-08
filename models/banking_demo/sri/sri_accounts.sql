{{ config(
    incremental_strategy = 'insert_overwrite',
    partition_by = {
      "field": "position_date",
      "data_type": "date"
    },
    cluster_by = ["customer_id", "account_id"]
) }}

SELECT 
    CURRENT_DATE('Asia/Jakarta') AS position_date,
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
FROM {{ source('demo_banking_stg', 'stg_accounts') }}