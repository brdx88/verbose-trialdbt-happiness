{{ config(
    cluster_by=['payment_id']
) }}

select
    CAST('2025-10-07' AS DATE) as position_date,
    p.payment_id,
    p.loan_id,
    p.payment_date,
    p.payment_amount,
    p.principal_component,
    p.interest_component,
    p.penalty_component,
    p.payment_method,
    p.payment_status,

    -- Derived metrics
    case 
        when p.payment_amount > 0 then round(p.principal_component / p.payment_amount, 4)
        else null
    end as principal_ratio,

    case 
        when p.payment_amount > 0 then round(p.interest_component / p.payment_amount, 4)
        else null
    end as interest_ratio,

    case 
        when p.payment_amount > 0 then round(p.penalty_component / p.payment_amount, 4)
        else null
    end as penalty_ratio,

    -- Flags
    case 
        when upper(p.payment_status) = 'COMPLETED' then 1
        else 0
    end as is_completed_payment,

    case
        when upper(p.payment_status) = 'FAILED' then 1
        else 0
    end as is_failed_payment

from {{ ref('stg_payments') }} as p