select
    CAST('2025-10-10' AS DATE) AS position_date,
    customer_id,
    name,
    n_unique_loans,
    average_interest_rate,
    last_payment_date,
    ROUND(total_loan_amount, 2) AS total_loan_amount,
    ROUND(total_payment_amount, 2) AS total_payment_amount,
    ROUND(total_payment_amount / total_loan_amount, 2) as repayment_ratio
from {{ ref('int_risk_loans_summary') }}
