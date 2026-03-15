with tenancies as (
    select * from {{ ref('stg_tenancies') }}
    where is_valid = true  -- exclude cancelled
),

date_spine as (
    select * from {{ ref('int_date_spine') }}
),

-- expand tenancies to daily grain
daily_occupancy as (
    select
        d.calendar_date,
        d.month_start,
        t.room_id,
        t.tenancy_id
    from date_spine d
    inner join tenancies t
        on d.calendar_date >= t.check_in_date
        and d.calendar_date < t.check_out_date  -- checkout date doesnt count
),

-- dedup: handle overlap tenancy (e.g. t_010 & t_011)
deduped as (
    select distinct
        calendar_date,
        month_start,
        room_id
    from daily_occupancy
)

select * from deduped