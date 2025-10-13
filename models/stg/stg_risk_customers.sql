WITH customers AS (
    SELECT
        CAST(region as INTEGER) as region,
        CAST(age as INTEGER) as age,
        CAST(name as STRING) as name,
        CAST(income as FLOAT64) as income,
        CAST(customer_id as STRING) as customer_id
    FROM {{ source('banking_bronze', 'risk_customers_raw') }}
)

SELECT * FROM customers
