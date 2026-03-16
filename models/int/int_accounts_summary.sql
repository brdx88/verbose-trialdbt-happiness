WITH ACCOUNTS as 
(
    SELECT *
    FROM {{ ref('stg_accounts') }}
)

, AGG as 
(
    SELECT
        CAST('2025-10-07' AS DATE) as position_date,
        customer_id,
        count(*) as total_accounts,
        sum(balance) as total_balance,
        avg(balance) as avg_balance,
        max(CASE WHEN account_status = 'ACTIVE' THEN 1 ELSE 0 END) as has_active_account
    FROM accounts
    GROUP BY customer_id
)

SELECT * 
FROM agg