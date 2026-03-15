with properties as (
    select * from {{ ref('stg_properties') }}
),

rooms as (
    select * from {{ ref('stg_rooms') }}
),

date_spine as (
    select * from {{ ref('int_date_spine') }}
),

room_with_property as (
    select
        r.room_id,
        r.property_id,
        r.room_type,
        r.deleted_at                                        as room_deleted_at,
        p.property_name,
        p.lease_start_date,
        p.lease_end_date,
        p.deleted_at                                        as property_deleted_at
    from rooms r
    left join properties p on r.property_id = p.property_id
),

daily_availability as (
    select
        d.calendar_date,
        d.month_start,
        r.room_id,
        r.property_id,
        r.property_name
    from date_spine d
    inner join room_with_property r
        on d.calendar_date >= r.lease_start_date
        and d.calendar_date < r.lease_end_date
        and (r.room_deleted_at is null or d.calendar_date < r.room_deleted_at)
        and (r.property_deleted_at is null or d.calendar_date < r.property_deleted_at)
)

select * from daily_availability