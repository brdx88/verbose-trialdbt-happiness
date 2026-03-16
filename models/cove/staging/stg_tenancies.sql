WITH SOURCE as 
(
    SELECT * FROM {{ source('cove_raw', 'tenancies') }}
)

, CLEANED as 
(
    -- unify the data type columns for standardization and better join on the downstream.
    SELECT
        CAST(_id AS STRING) as tenancy_id,
        CAST(roomId AS STRING) as room_id,
        CAST(tenant_id AS STRING) AS tenant_id,
        DATE(checkInDate) as check_in_date,
        DATE(checkOutDate) as check_out_date,
        UPPER(CAST(status AS STRING)) AS status,

        -- exclude cancelled FROM occupancy calculation downstream
        CASE
            WHEN UPPER(status) = 'CANCELLED' THEN false                 -- using `UPPER()` or `LOWER()` for making the value is directed into our rules as we expected.
            ELSE true
        END as is_valid,

        timestamp(updatedAt) as updated_at

    FROM source
)

-- intentionally not using '*' for the best practice for better performance
SELECT
    tenancy_id,
    room_id,
    tenant_id,
    check_in_date,
    check_out_date,
    status,
    is_valid,
    updated_at
FROM CLEANED