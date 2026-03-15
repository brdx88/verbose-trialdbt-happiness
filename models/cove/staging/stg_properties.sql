with source as (
    select * from {{ source('cove_raw', 'properties') }}
),

renamed as (
    select
        _id                                                         as property_id,
        name                                                        as property_name,
        city,
        date(lease_start_date)                                      as lease_start_date,
        date(lease_end_date)                                        as lease_end_date,

        -- normalize deletedAt: handle both ISO and non-ISO format
        case
            when deletedAt is null then null
            else date(timestamp(replace(CAST(deletedAt AS STRING), ' ', 'T')))
        end                                                         as deleted_at,

        -- is this property still active?
        case
            when deletedAt is null then true
            else false
        end                                                         as is_active,

        timestamp(updatedAt)                                        as updated_at

    from source
)

select * from renamed