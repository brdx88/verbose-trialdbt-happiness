SELECT
    account_id,
    customer_id,
    account_type,
    currency,
    balance,
    status,
    open_date,
    close_date,
    branch_id,
    risk_score
FROM {{ source('banking_bronze', 'accounts') }}
WHERE _PARTITIONDATE = '2025-10-02'