WITH CUSTOMERS as 
(
    SELECT * FROM {{ ref('stg_customers') }}
)

, ACCOUNTS_SUMMARY as 
(
    SELECT * FROM {{ ref('int_accounts_summary') }}
    WHERE position_date = '2025-10-07'
)

, FINAL as 
(
    SELECT
        current_date() as position_date,
        c.customer_id,
        c.full_name,
        c.city,
        c.country,
        COALESCE(a.total_accounts, 0) AS total_accounts,
        COALESCE(a.total_balance, 0) AS total_balance,
        COALESCE(a.avg_balance, 0) AS avg_balance,
        case
            when a.total_balance >= 100000000 then 'VIP'
            when a.total_balance >= 10000000 then 'PREMIUM'
            else 'REGULAR'
        end as customer_tier
    FROM customers c
    LEFT JOIN accounts_summary a
        ON c.customer_id = a.customer_id
)

SELECT * FROM FINAL
