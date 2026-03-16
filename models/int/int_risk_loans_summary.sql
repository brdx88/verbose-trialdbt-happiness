with loans as (
    select * from {{ ref('stg_risk_loans') }}
),
customers as (
    select * from {{ ref('stg_risk_customers') }}
),
payments as (
    select * from {{ ref('stg_risk_payments') }}
)

select
    CAST('2025-10-10' AS DATE) as position_date,
    c.customer_id,
    c.name,
    COALESCE(COUNT(DISTINCT l.loan_id), 0) AS n_unique_loans,
    COALESCE(SUM(l.loan_amount), 0) as total_loan_amount,
    COALESCE(AVG(l.interest_rate), 0) as average_interest_rate,
    MAX(l.loan_date) as latest_loan_date,
    COALESCE(SUM(p.payment_amount), 0) as total_payment_amount,
    MAX(p.payment_date) as last_payment_date
from customers c
left join loans l on l.customer_id = c.customer_id
left join payments p on l.loan_id = p.loan_id
group by
    c.customer_id,
    c.name