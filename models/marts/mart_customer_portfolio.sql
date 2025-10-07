with customers as (
    select * from {{ ref('stg_customers') }}
),
accounts_summary as (
    select * from {{ ref('int_accounts_summary') }}
),
final as (
    select
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
    from customers c
    left join accounts_summary a
        on c.customer_id = a.customer_id
)

select * from final
