-- test untuk: "Apakah hari ini ada data yang berhasil masuk ke partition today?"
    -- 0 rows returned  → PASS
    -- >=1 rows returned → FAIL
        -- kalau data hari ini TIDAK ADA
        -- → return row
        -- → FAIL

with sri_today_check as (

    select 'sri_customers' as model_name
    from (
        select 1
    )
    where not exists (
        select 1
        from {{ ref('sri_customers') }}
        where position_date = current_date('Asia/Jakarta')
    )

    union all

    select 'sri_accounts' as model_name
    from (
        select 1
    )
    where not exists (
        select 1
        from {{ ref('sri_accounts') }}
        where position_date = current_date('Asia/Jakarta')
    )

    union all

    select 'sri_loans' as model_name
    from (
        select 1
    )
    where not exists (
        select 1
        from {{ ref('sri_loans') }}
        where position_date = current_date('Asia/Jakarta')
    )

)

select *
from sri_today_check