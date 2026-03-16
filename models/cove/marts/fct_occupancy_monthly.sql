-- PURPOSE: 
    -- final mart table showing monthly occupancy rate per property.

-- GRAIN LEVEL: 
    -- one row per property per month.

-- FORMULA OF OCCUPANCY RATE: 
    -- occupancy_rate_pct = occupied_room_nights / available_room_nights * 100%

-- ASSUMPTION: 
    -- overlapping tenancies are already deduplicated in int_room_occupancy,
    -- so occupied_room_nights will never exceed available_room_nights.

-- partitioned by `month_start` to reducing query cost and improving performance along the way.
{{ config(
    materialized='table',
    partition_by=
    {
        'field': 'month_start', 'data_type': 'date'
    }
) }}

WITH AVAILABILITY AS 
(
    SELECT * FROM {{ ref('int_room_availability') }}
)

, OCCUPANCY AS 
(
    SELECT * FROM {{ ref('int_room_occupancy') }}
)

, MONTHLY AS 
(
    SELECT
        a.month_start,
        a.property_id,
        a.property_name,
        COUNT(DISTINCT a.room_id) AS total_rooms,
        COUNT(a.calendar_date) AS available_room_nights,
        COUNT(o.room_id) AS occupied_room_nights
    FROM AVAILABILITY a
    LEFT JOIN OCCUPANCY o
        ON a.calendar_date = o.calendar_date
        AND a.room_id = o.room_id
    GROUP BY
        a.month_start,
        a.property_id,
        a.property_name
)

SELECT
    month_start,
    property_id,
    property_name,
    total_rooms,
    available_room_nights,
    occupied_room_nights,

    ROUND(
        SAFE_DIVIDE(occupied_room_nights, available_room_nights) * 100,
        2
    ) as occupancy_rate_pct

FROM MONTHLY