SELECT
    CAST(branch_id AS INTEGER) AS branch_id,
    CAST(UPPER(branch_name) AS STRING) AS branch_name,
    CAST(UPPER(city) AS STRING) AS city,
    CAST(UPPER(province) AS STRING) AS province,
    CAST(UPPER(country) AS STRING) AS country,
    CAST(UPPER(manager_name) AS STRING) AS manager_name,
    CAST(open_date AS DATE) AS open_date,
    CAST(employee_count AS INTEGER) AS employee_count
FROM {{ source('banking_bronze', 'branches') }}
WHERE _PARTITIONDATE = '2025-10-02'