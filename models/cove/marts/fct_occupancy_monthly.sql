with availability as (
    select * from {{ ref('int_room_availability') }}
),

occupancy as (
    select * from {{ ref('int_room_occupancy') }}
),

-- join occupancy ke availability, agregasi ke monthly
monthly as (
    select
        a.month_start,
        a.property_id,
        a.property_name,
        count(distinct a.room_id)                           as total_rooms,
        sum(1)                                              as available_room_nights,
        count(distinct case
            when o.room_id is not null
            then concat(cast(a.calendar_date as string), '_', a.room_id)
        end)                                                as occupied_room_nights
    from availability a
    left join occupancy o
        on a.calendar_date = o.calendar_date
        and a.room_id = o.room_id
    group by 1, 2, 3
),

final as (
    select
        month_start,
        property_id,
        property_name,
        total_rooms,
        available_room_nights,
        occupied_room_nights,
        round(
            safe_divide(occupied_room_nights, available_room_nights) * 100,
            2
        )                                                   as occupancy_rate_pct
    from monthly
)

select * from final