SELECT *
FROM {{ source('banking_bronze', 'transactions') }}
WHERE _PARTITIONDATE = '2025-10-02'