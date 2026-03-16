WITH SOURCE as 
(
    SELECT * FROM {{ source('cove_raw', 'properties') }}
)

, CLEANED as 
(
    -- unify the data type columns for standardization and better join on the downstream.
    SELECT
        CAST(_id AS STRING) as property_id,
        UPPER(CAST(name AS STRING)) as property_name,
        UPPER(CAST(city AS STRING)) AS city,
        DATE(lease_start_date) as lease_start_date,
        DATE(lease_end_date) as lease_end_date,
        DATE(deletedAt) as deleted_at,

        -- add flag column for 'is this property still active'?
        CASE
            WHEN deletedAt IS NULL THEN true
            ELSE false
        END as is_active,

        timestamp(updatedAt) as updated_at

    FROM SOURCE
)

-- intentionally not using '*' for the best practice for better performance
SELECT
    property_id,
    property_name,
    city,
    lease_start_date,
    lease_end_date,
    deleted_at,
    is_active,
    updated_at
FROM CLEANED