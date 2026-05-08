{{ config(
    incremental_strategy = 'insert_overwrite',
    partition_by = {
      "field": "position_date",
      "data_type": "date"
    },
    cluster_by = ["customer_id", "loan_id"]
) }}

select
    l.position_date,
    l.loan_id,
    l.customer_id,
    l.loan_type,
    l.principal_amount,
    l.interest_rate,
    l.tenor_months,
    l.disbursement_date,
    l.maturity_date,
    l.outstanding_balance,
    l.loan_status,

    -- Calculate remaining principal percentage
    case
        when l.principal_amount > 0 then round(l.outstanding_balance / l.principal_amount, 4)
        else null
    end as remaining_balance_ratio,

    -- Flag if active
    case 
        when l.loan_status = 'ACTIVE' and l.outstanding_balance > 0 then 1
        else 0
    end as is_active_loan,

    -- Derived column: loan_age_in_months
    case 
        when l.disbursement_date is not null then timestamp_diff(current_date(), l.disbursement_date, month)
        else null
    end as loan_age_in_months

from {{ ref('sri_loans') }} as l