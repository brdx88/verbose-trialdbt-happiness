WITH SOURCE as 
(
    SELECT * FROM {{ source('cove_raw', 'rooms') }}
)

, CLEANED as 
(
    -- unify the data type columns for standardization and better join on the downstream.
    SELECT
        CAST(_id AS STRING) as room_id,
        CAST(propertyId AS STRING) as property_id,
        CAST(room_number AS STRING) AS room_number,
        UPPER(CAST(type AS STRING)) as room_type,
        DATE(deletedAt) as deleted_at,

        -- add flag column for 'is this room still active'?
        CASE
            WHEN deletedAt IS NULL THEN true
            ELSE false
        END as is_active,

        timestamp(updatedAt) as updated_at

    FROM source
)

-- intentionally not using '*' for the best practice for better performance
SELECT
    room_id,
    property_id,
    room_number,
    room_type,
    deleted_at,
    is_active,
    updated_at
FROM CLEANED