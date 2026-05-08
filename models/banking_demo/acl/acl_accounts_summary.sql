{{ config(
    incremental_strategy = 'insert_overwrite',
    partition_by = {
      "field": "position_date",
      "data_type": "date"
    },
    cluster_by = ["customer_id"]
) }}

WITH ACCOUNTS as 
(
    SELECT *
    FROM {{ ref('sri_accounts') }}
)

, AGG as 
(
    SELECT
        position_date,
        customer_id,
        count(*) as total_accounts,
        sum(balance) as total_balance,
        avg(balance) as avg_balance,
        max(CASE WHEN account_status = 'ACTIVE' THEN 1 ELSE 0 END) as has_active_account
    FROM accounts
    GROUP BY 
        position_date,
        customer_id
)

SELECT * 
FROM agg