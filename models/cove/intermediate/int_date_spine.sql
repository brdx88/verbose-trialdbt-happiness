with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2025-01-01' as date)",
        end_date="cast('2026-12-31' as date)"
    ) }}
),

renamed as (
    select
        cast(date_day as date)  as calendar_date,
        extract(year from date_day)                         as year,
        extract(month from date_day)                        as month,
        date_trunc(cast(date_day as date), month)           as month_start
    from date_spine
)

select * from renamed