SELECT *
FROM {{source('sandbox','dim__cbd__customer')}}
WHERE as_of_date = '2025-09-10'