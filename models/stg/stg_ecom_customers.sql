with 

source as (

    select * from {{ source('banking_bronze', 'ecom_customers') }}

),

renamed as (

    select
        signup_date,
        last_name,
        email,
        first_name,
        customer_id

    from source

)

select * from renamed