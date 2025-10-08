WITH CUSTOMERS as 
(
    SELECT * FROM {{ ref('int_customers') }}
    WHERE position_date = '2025-10-07'
)

, ACCOUNTS_SUMMARY as 
(
    SELECT *
    FROM {{ ref('int_accounts_summary') }}
    WHERE position_date = '2025-10-07'
)

, LOANS_SUMMARY as
(
    SELECT
        customer_id,
        COUNT(DISTINCT loan_id) as n_unique_loan,
        SUM(principal_amount) as total_principal_amount,
        AVG(interest_rate) as average_interest_rate,
        MAX(tenor_months) as longest_tenor_months,
        MIN(tenor_months) as quickest_tenor_months,
        SUM(outstanding_balance) as total_outstanding_balance,
        
        case
            when SUM(principal_amount) > 0 then round(SUM(outstanding_balance) / SUM(principal_amount), 2)
            else null
        end as remaining_balance_ratio

    FROM {{ ref('int_loans_summary') }}
    WHERE position_date = '2025-10-07'
    GROUP BY customer_id
)

, FINAL as 
(
    SELECT
        CAST('2025-10-07' AS DATE) as position_date,
        CUSTOMERS.customer_id,
        CUSTOMERS.full_name,
        CUSTOMERS.gender,
        CUSTOMERS.city,
        CUSTOMERS.province,
        CUSTOMERS.country,
        CUSTOMERS.date_of_birth,
        CUSTOMERS.join_date,
        CUSTOMERS.customer_segment,
        CUSTOMERS.kyc_status,
        CUSTOMERS.age,
        CUSTOMERS.is_new_customer,
        CUSTOMERS.is_kyc_verified,

        COALESCE(ACCOUNTS_SUMMARY.total_accounts, 0) AS total_accounts,
        COALESCE(ACCOUNTS_SUMMARY.total_balance, 0) AS total_balance,
        COALESCE(ACCOUNTS_SUMMARY.avg_balance, 0) AS avg_balance,
        
        case
            when ACCOUNTS_SUMMARY.total_balance >= 100000000 then 'VIP'
            when ACCOUNTS_SUMMARY.total_balance >= 10000000 then 'PREMIUM'
            else 'REGULAR'
        end as customer_tier,

        COALESCE(LOANS_SUMMARY.n_unique_loan,0) AS n_unique_loan,
        COALESCE(LOANS_SUMMARY.average_interest_rate,0) AS average_interest_rate,
        COALESCE(LOANS_SUMMARY.longest_tenor_months,0) AS longest_tenor_months,
        COALESCE(LOANS_SUMMARY.quickest_tenor_months,0) AS quickest_tenor_months,
        COALESCE(LOANS_SUMMARY.remaining_balance_ratio,0) AS remaining_balance_ratio

    FROM CUSTOMERS
    LEFT JOIN ACCOUNTS_SUMMARY
        ON CUSTOMERS.customer_id = ACCOUNTS_SUMMARY.customer_id
    LEFT JOIN LOANS_SUMMARY
        ON CUSTOMERS.customer_id = LOANS_SUMMARY.customer_id
)

SELECT * FROM FINAL
