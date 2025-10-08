{{ config(
    cluster_by=['account_id']
) }}

with transactions as (
    select
        CAST('2025-10-07' AS DATE) as position_date,
        t.transaction_id,
        t.account_id,
        t.transaction_date,
        t.transaction_type,
        t.amount,
        t.currency,
        t.merchant,
        t.channel,
        t.transaction_status,
        t.location,

        -- Standardize status and type
        case
            when upper(t.transaction_status) in ('SUCCESS', 'COMPLETED') then 'SUCCESS'
            when upper(t.transaction_status) in ('FAILED', 'DECLINED') then 'FAILED'
            else 'PENDING'
        end as standardized_status,

        case
            when upper(t.transaction_type) in ('DEPOSIT', 'CREDIT') then 'CREDIT'
            when upper(t.transaction_type) in ('WITHDRAWAL', 'DEBIT', 'PURCHASE') then 'DEBIT'
            else 'OTHER'
        end as standardized_type,

        -- Flags
        case when upper(t.transaction_status) in ('SUCCESS', 'COMPLETED') then 1 else 0 end as is_success,
        case when upper(t.transaction_status) in ('FAILED', 'DECLINED') then 1 else 0 end as is_failed,
        case when upper(t.channel) in ('ATM', 'BRANCH') then 'OFFLINE' else 'ONLINE' end as channel_group,

        -- Derived metrics
        abs(t.amount) as absolute_amount
    from {{ ref('stg_transactions') }} as t
)

select *
from transactions
