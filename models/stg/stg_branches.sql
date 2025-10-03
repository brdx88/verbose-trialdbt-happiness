SELECT
    branch_id,
    branch_name,
    city,
    province,
    country,
    manager_name,
    open_date,
    employee_count
FROM {{ source('banking_bronze', 'branches') }}
WHERE _PARTITIONDATE = '2025-10-02'