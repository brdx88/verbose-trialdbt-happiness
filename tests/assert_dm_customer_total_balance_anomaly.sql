with daily_metrics as (

    select
        position_date,
        sum(total_account_balance) as total_account_balance
    from {{ ref('golden_customer_datamart') }}
    where position_date in (
        current_date('Asia/Jakarta'),
        date_sub(current_date('Asia/Jakarta'), interval 1 day)
    )
    group by position_date

),

comparison as (

    select
        curr.total_account_balance as current_total_account_balance,
        prev.total_account_balance as previous_total_account_balance,

        safe_divide(
            abs(curr.total_account_balance - prev.total_account_balance),
            nullif(prev.total_account_balance, 0)
        ) as diff_ratio

    from daily_metrics curr
    join daily_metrics prev
        on curr.position_date = current_date('Asia/Jakarta')
       and prev.position_date = date_sub(current_date('Asia/Jakarta'), interval 1 day)

)

select *
from comparison
where diff_ratio > 0.20