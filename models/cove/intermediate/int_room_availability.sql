-- PURPOSE?
    -- expand each room into a daily-grain-calendar showing when each room is 'available'.

-- ASSUMPTIONS: 
    -- a room is considered "available" on a given day if ALL of the following are true:
        -- 1. the day falls within the property's lease period "(lease_start_date <= day < lease_end_date)"
        -- 2. the room has not been soft-deleted* on or before that day "(room_deleted_at IS NULL OR day < room_deleted_at)"
        -- 3. the property has not been soft-deleted* on or before that day "(property_deleted_at IS NULL OR day < property_deleted_at)"
    
    -- *soft-deleted: data isn't actualy deleted in the database but flagged as 'deleted'

-- EXAMPLE: 
    -- r_201 (Cove Bugis) has room_deleted_at = 2025-12-31.
    -- so r_201 is available on 2025-12-30, but NOT on 2025-12-31 onwards.
    -- p_003 (Cove Joo Chiat) has property_deleted_at = 2025-12-01.
    -- so all rooms under p_003 are unavailable from 2025-12-01 onwards.

-- partitioned by `calendar_date` to reducing query cost and improving performance along the way.
{{ config(
    materialized='table',
    partition_by=
    {
        'field': 'calendar_date', 'data_type': 'date'
    }
) }}

WITH PROPERTIES AS 
(
    SELECT * FROM {{ ref('stg_properties') }}
)

, ROOMS AS 
(
    SELECT * FROM {{ ref('stg_rooms') }}
)

, DATE_SPINE AS 
(
    SELECT * FROM {{ ref('int_date_spine') }}
)

, ROOM_WITH_PROPERTY AS 
(
    SELECT
        r.room_id,
        r.property_id,
        r.room_type,
        r.deleted_at AS room_deleted_at,
        p.property_name,
        p.lease_start_date,
        p.lease_end_date,
        p.deleted_at AS property_deleted_at
    FROM ROOMS r
    LEFT JOIN PROPERTIES p
        ON r.property_id = p.property_id
)

, DAILY_AVAILABILITY AS 
(
    SELECT
        d.calendar_date,
        d.month_start,
        r.room_id,
        r.property_id,
        r.property_name
    FROM DATE_SPINE d
    INNER JOIN ROOM_WITH_PROPERTY r
        ON d.calendar_date >= r.lease_start_date
        AND d.calendar_date < r.lease_end_date
        AND (r.room_deleted_at IS NULL OR d.calendar_date < r.room_deleted_at)
        AND (r.property_deleted_at IS NULL OR d.calendar_date < r.property_deleted_at)
)

SELECT
    calendar_date,
    month_start,
    room_id,
    property_id,
    property_name
FROM DAILY_AVAILABILITY