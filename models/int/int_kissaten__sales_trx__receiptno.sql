{{
    config(
        cluster_by='no_penjualan'
    )
}}

SELECT
    CAST('2025-11-13' AS DATE) AS position_date,
    CAST(REGEXP_REPLACE(waktu_penjualan, r" WIB$", "") AS TIME) as waktu_penjualan_clean,
    *
FROM {{ ref('stg_kissaten__sales_trx__receiptno') }}
WHERE no_tanda_terima <> ''