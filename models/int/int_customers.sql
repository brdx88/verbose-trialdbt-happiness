{{ config(
    cluster_by=['customer_id']
) }}

with base as (
    select
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
    from {{ ref('stg_customers') }}
),

feature_engineering as (
    select
        CAST('2025-10-07' AS DATE) as position_date,
        *,
        -- derived features
        date_diff(current_date, date_of_birth, year) as age,
        date_diff(current_date, join_date, month) as tenure_months,

        -- flag if customer is newly joined (< 6 months)
        case 
            when date_diff(current_date, join_date, month) < 6 then 1 
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
