{{ config(
    incremental_strategy = 'insert_overwrite',
    partition_by = {
      "field": "position_date",
      "data_type": "date"
    },
    cluster_by = ["customer_id"]
) }}

with base as (
    select
        position_date,
        customer_id,
        full_name,
        gender,
        city,
        province,
        country,
        date_of_birth,
        join_date,
        customer_segment,
        kyc_status
    from {{ ref('sri_customers') }}
),

feature_engineering as (
    select
        position_date,
        customer_id,
        full_name,
        gender,
        city,
        province,
        country,
        date_of_birth,
        join_date,
        customer_segment,
        kyc_status,

        -- derived features
        date_diff(position_date, date_of_birth, year) as age,
        date_diff(position_date, join_date, month) as tenure_months,

        -- flag if customer is newly joined (< 6 months)
        case 
            when date_diff(position_date, join_date, month) < 6 then 1
            else 0 
        end as is_new_customer,

        -- flag if KYC is completed
        case 
            when upper(kyc_status) = 'VERIFIED' then 1 
            else 0 
        end as is_kyc_verified
    from base
)

select * from feature_engineering
