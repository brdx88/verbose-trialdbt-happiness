with 

source as (

    select * from {{ source('banking_bronze', 'ecom_products') }}

),

renamed as (

    select
        price,
        category,
        product_name,
        product_id

    from source

)

select * from renamed