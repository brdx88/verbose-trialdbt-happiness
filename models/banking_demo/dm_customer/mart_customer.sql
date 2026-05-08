-- models/marts/golden_customer_datamart.sql

{{ config(
    incremental_strategy = 'insert_overwrite',
    partition_by = {
      "field": "position_date",
      "data_type": "date"
    },
    cluster_by = ["customer_id", "customer_segment", "kyc_status"]
) }}

with customers as (

    select
        position_date,
        customer_id,
        full_name,
        gender,
        city,
        province,
        country,
        date_of_birth,
        join_date,
        customer_segment,
        kyc_status,
        age,
        tenure_months,
        is_new_customer,
        is_kyc_verified
    from {{ ref('acl_customers') }}

),

accounts as (

    select
        position_date,
        customer_id,

        sum(total_accounts) as total_accounts,
        sum(total_balance) as total_account_balance,
        avg(avg_balance) as avg_account_balance,
        max(has_active_account) as has_active_account

    from {{ ref('acl_accounts_summary') }}
    group by 1, 2

),

loans as (

    select
        position_date,
        customer_id,

        count(distinct loan_id) as total_loans,
        countif(is_active_loan = 1) as active_loans,

        sum(principal_amount) as total_principal_amount,
        sum(outstanding_balance) as total_outstanding_loan_balance,

        avg(interest_rate) as avg_interest_rate,
        avg(tenor_months) as avg_tenor_months,
        avg(remaining_balance_ratio) as avg_remaining_balance_ratio,
        avg(loan_age_in_months) as avg_loan_age_months,

        min(disbursement_date) as first_loan_disbursement_date,
        max(disbursement_date) as latest_loan_disbursement_date,
        min(maturity_date) as nearest_maturity_date,

        countif(lower(loan_status) = 'active') as active_loan_count,
        countif(lower(loan_status) = 'closed') as closed_loan_count,
        countif(lower(loan_status) = 'default') as default_loan_count

    from {{ ref('acl_loans_summary') }}
    group by 1, 2

),

final as (

    select
        c.position_date,
        c.customer_id,

        -- customer profile
        c.full_name,
        c.gender,
        c.city,
        c.province,
        c.country,
        c.date_of_birth,
        c.join_date,
        c.customer_segment,
        c.kyc_status,
        c.age,
        c.tenure_months,
        c.is_new_customer,
        c.is_kyc_verified,

        -- account metrics
        coalesce(a.total_accounts, 0) as total_accounts,
        coalesce(a.total_account_balance, 0) as total_account_balance,
        coalesce(a.avg_account_balance, 0) as avg_account_balance,
        coalesce(a.has_active_account, 0) as has_active_account,

        -- loan metrics
        coalesce(l.total_loans, 0) as total_loans,
        coalesce(l.active_loans, 0) as active_loans,
        coalesce(l.total_principal_amount, 0) as total_principal_amount,
        coalesce(l.total_outstanding_loan_balance, 0) as total_outstanding_loan_balance,
        coalesce(l.avg_interest_rate, 0) as avg_interest_rate,
        coalesce(l.avg_tenor_months, 0) as avg_tenor_months,
        coalesce(l.avg_remaining_balance_ratio, 0) as avg_remaining_balance_ratio,
        coalesce(l.avg_loan_age_months, 0) as avg_loan_age_months,
        l.first_loan_disbursement_date,
        l.latest_loan_disbursement_date,
        l.nearest_maturity_date,
        coalesce(l.active_loan_count, 0) as active_loan_count,
        coalesce(l.closed_loan_count, 0) as closed_loan_count,
        coalesce(l.default_loan_count, 0) as default_loan_count,

        -- golden customer flags
        case
            when coalesce(a.total_accounts, 0) > 0 then 1
            else 0
        end as is_account_holder,

        case
            when coalesce(l.total_loans, 0) > 0 then 1
            else 0
        end as is_borrower,

        case
            when coalesce(a.total_account_balance, 0) > 0
              or coalesce(l.total_outstanding_loan_balance, 0) > 0
            then 1
            else 0
        end as is_financially_active,

        -- simple profitability / exposure proxy
        coalesce(a.total_account_balance, 0)
          - coalesce(l.total_outstanding_loan_balance, 0)
            as net_customer_balance

    from customers c

    left join accounts a
        on c.customer_id = a.customer_id
       and c.position_date = a.position_date

    left join loans l
        on c.customer_id = l.customer_id
       and c.position_date = l.position_date

)

select *
from final