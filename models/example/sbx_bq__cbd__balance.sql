SELECT *
FROM {{ source('sandbox','dim__cbd__balance') }}
WHERE as_of_date = '2025-09-10'