{{
    config(
        cluster_by='jam'
    )
}}

SELECT
    position_date,
    FORMAT_DATE('%A', tanggal_penjualan) AS hari,
    
    EXTRACT(HOUR FROM waktu_penjualan_clean) AS jam,
    SUM(jumlah_bersih) as total_jumlah_bersih

FROM {{ ref('int_kissaten__sales_trx__receiptno') }}
WHERE position_date = '2025-11-13'

GROUP BY
    position_date,
    hari,
    jam