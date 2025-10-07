with accounts as (
    select *
    from {{ ref('stg_accounts') }}
),

agg as (
    select
        customer_id,
        count(*) as total_accounts,
        sum(balance) as total_balance,
        avg(balance) as avg_balance,
        max(case when account_status = 'ACTIVE' then 1 else 0 end) as has_active_account
    from accounts
    group by customer_id
)

select * from agg