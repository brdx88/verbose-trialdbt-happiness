{{
    config(
        cluster_by='branch_id'
    )
}}

select
    CAST('2025-10-07' AS DATE) as position_date,
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

from {{ ref('stg_branches') }}