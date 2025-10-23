with 

source as (

    select * from {{ source('banking_bronze', 'ecom_customers') }}

),

renamed as (

    select
        DATE(TIMESTAMP_MILLIS(signup_date)) as signup_date,
        last_name,
        email,
        first_name,
        customer_id

    from source

)

select * from renamed