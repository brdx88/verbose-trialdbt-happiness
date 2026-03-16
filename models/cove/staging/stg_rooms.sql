with source as (
    select * from {{ source('cove_raw', 'rooms') }}
),

renamed as (
    select
        _id                                                         as room_id,
        propertyId                                                  as property_id,
        room_number,
        type                                                        as room_type,

        -- normalize deletedAt
        case
            when deletedAt is null then null
            else date(timestamp(replace(CAST(deletedAt AS STRING), ' ', 'T')))
        end                                                         as deleted_at,

        -- is this room still active?
        case
            when deletedAt is null then true
            else false
        end                                                         as is_active,

        timestamp(updatedAt)                                        as updated_at

    from source
)

select * from renamed