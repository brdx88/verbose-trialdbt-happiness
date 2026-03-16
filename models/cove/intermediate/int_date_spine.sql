-- PURPOSE?
    -- generate a 'calendar table', one row per day from 2025-01-01 to 2026-12-31.

-- WHY?
    -- tenancy data only has rows when a booking EXISTS. 
    -- if a room is vacant for an entire month, there would be no row for that month,
    -- "making it impossible to calculate available_room_nights" or show 0% occupancy correctly.
    -- the idea is "by joining to this date spine", every day is explicitly represented regardless
    -- of whether a booking exists.

-- EXAMPLE:
    -- `r_301` has no tenancy in `Dec 2025`. without date spine, `Dec 2025` would be missing
    -- from the final occupancy table. with date spine, it appears with 0 occupied nights.

WITH DATE_SPINE AS 
(
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="CAST('2025-01-01' AS DATE)",
        end_date="CAST('2026-12-31' AS DATE)"
    ) }}
)

SELECT
    CAST(date_day AS DATE) as calendar_date,
    EXTRACT(YEAR FROM date_day) as year,
    EXTRACT(MONTH FROM date_day) AS month,
    DATE_TRUNC(CAST(date_day AS DATE), MONTH) AS month_start
FROM DATE_SPINE