-- PURPOSE: 
    -- expand each "valid tenancy" into a daily-grain-calendar showing when each room is 'occupied'.

-- ASSUMPTIONS:

    -- ASSUMPTION 1: 
        -- cancelled tenancies are excluded from occupancy calculation.
        -- only status != 'CANCELLED' tenancies are considered valid.
            -- EXAMPLE: 
                -- t_015 (r_302, status='cancelled') is excluded
                -- r_302 is treated as vacant for that period.

    -- ASSUMPTION 2: 
        -- checkout date is NOT counted as an occupied night.
        -- a tenant checking out on 2025-06-30 frees the room on that day.
            -- EXAMPLE: 
                -- t_002 checks out 2025-06-30, so Jun 30 is NOT occupied by t_002.
                -- t_003 checks in 2025-07-15, so the room is vacant Jul 1–14.

    -- ASSUMPTION 3: 
        -- overlapping tenancies on the same room are deduplicated via `SELECT DISTINCT` on `room_id` level.
        -- counted as 1 occupied night, not 2; so occupied room nights will never exceed available room nights later.
            -- EXAMPLE: 
                -- t_010 (checkout 2025-07-01) and t_011 (checkin 2025-06-25) overlap
                -- on r_202 for 6 days (Jun 25–30). these 6 days are counted once, not twice.

-- partitioned by `month_start` to reducing query cost and improving performance along the way.
{{ config(
    materialized='table',
    partition_by=
    {
        'field': 'month_start', 'data_type': 'date'
    }
) }}

WITH TENANCIES AS 
(
    SELECT * FROM {{ ref('stg_tenancies') }}
    WHERE is_valid = true       -- ASSUMPTION 1
)

, DATE_SPINE AS 
(
    SELECT * FROM {{ ref('int_date_spine') }}
)

, DAILY_OCCUPANCY AS 
(
    SELECT
        d.calendar_date,
        d.month_start,
        t.room_id,
        t.tenancy_id
    FROM DATE_SPINE d
    INNER JOIN TENANCIES t
        ON d.calendar_date >= t.check_in_date
        AND d.calendar_date < t.check_out_date          -- ASSUMPTION 2
)

, DEDUPED AS 
(
    SELECT DISTINCT                                 -- ASSUMPTION 3
        calendar_date,
        month_start,
        room_id
    FROM DAILY_OCCUPANCY
)

SELECT
    calendar_date,
    month_start,
    room_id
FROM DEDUPED