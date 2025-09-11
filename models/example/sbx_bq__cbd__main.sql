SELECT *
FROM {{ source('sandbox', 'comprehensive_banking_dataset') }}
WHERE _PARTITIONDATE = '2025-09-09'
;