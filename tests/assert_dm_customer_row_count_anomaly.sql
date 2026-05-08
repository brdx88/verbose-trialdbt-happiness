-- Test ini akan FAIL kalau row count hari ini naik/turun lebih dari 20% dibanding kemarin.
    -- 0 rows = PASS
    -- >= 1 rows = FAIL / anomaly

with row_counts as (

    select
        position_date,
        count(*) as row_count
    from {{ ref('mart_customer') }}
    where position_date in (
        current_date('Asia/Jakarta'),
        date_sub(current_date('Asia/Jakarta'), interval 1 day)
    )
    group by position_date

),

comparison as (

    select
        curr.row_count as current_row_count,
        prev.row_count as previous_row_count,

        safe_divide(
            abs(curr.row_count - prev.row_count),
            prev.row_count
        ) as diff_ratio

    from row_counts curr
    join row_counts prev
        on curr.position_date = current_date('Asia/Jakarta')
       and prev.position_date = date_sub(current_date('Asia/Jakarta'), interval 1 day)

)

select *
from comparison
where diff_ratio > 0.20