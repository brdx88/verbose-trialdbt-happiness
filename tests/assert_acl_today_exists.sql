with acl_today_check as (

    select 'acl_customers' as model_name
    from (select 1)
    where not exists (
        select 1
        from {{ ref('acl_customers') }}
        where position_date = current_date('Asia/Jakarta')
    )

    union all

    select 'acl_accounts_summary' as model_name
    from (select 1)
    where not exists (
        select 1
        from {{ ref('acl_accounts_summary') }}
        where position_date = current_date('Asia/Jakarta')
    )

    union all

    select 'acl_loans_summary' as model_name
    from (select 1)
    where not exists (
        select 1
        from {{ ref('acl_loans_summary') }}
        where position_date = current_date('Asia/Jakarta')
    )

)

select *
from acl_today_check