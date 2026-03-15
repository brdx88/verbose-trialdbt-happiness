with source as (
    select * from {{ source('cove_raw', 'tenancies') }}
),

renamed as (
    select
        _id                                                         as tenancy_id,
        roomId                                                      as room_id,
        tenant_id,
        date(checkInDate)                                           as check_in_date,
        date(checkOutDate)                                          as check_out_date,
        status,

        -- exclude cancelled from occupancy calculation downstream
        case
            when status = 'cancelled' then false
            else true
        end                                                         as is_valid,

        timestamp(updatedAt)                                        as updated_at

    from source
)

select * from renamed