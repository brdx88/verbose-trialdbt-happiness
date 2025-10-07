{{
    config(
        cluster_by='branch_id'
    )
}}

with branches as (
    select * from {{ ref('stg_branches') }}
)

select
    current_date() as position_date,
    branch_id,
    branch_name,
    city,
    province,
    country,
    manager_name,
    open_date,
    employee_count,

    -- derived columns
    date_diff(current_date(), open_date, year) as branch_age_years,

    case
        when employee_count >= 100 then 'LARGE'
        when employee_count between 30 and 99 then 'MEDIUM'
        else 'SMALL'
    end as branch_size_category,

    concat(city, ', ', province) as region_label

from branches