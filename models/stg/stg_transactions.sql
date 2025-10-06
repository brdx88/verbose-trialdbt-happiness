SELECT
    CAST(transaction_id AS INTEGER) AS transaction_id,
    CAST(account_id AS INTEGER) AS account_id,
    CAST(transaction_date AS DATE) AS transaction_date,
    CAST(UPPER(transaction_type) AS STRING) AS transaction_type,
    CAST(amount AS INTEGER) AS amount,
    CAST(UPPER(currency) AS STRING) AS currency,
    CAST(COALESCE(UPPER(merchant), 'UNKNOWN') AS STRING) AS merchant,
    CAST(UPPER(channel) AS STRING) AS channel,
    CAST(UPPER(status) AS STRING) AS transaction_status,
    CAST(UPPER(location) AS STRING) AS location
FROM {{ source('banking_bronze', 'transactions') }}
WHERE _PARTITIONDATE = '2025-10-02'