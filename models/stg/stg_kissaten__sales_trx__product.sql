SELECT *
FROM {{ source('banking_bronze', 'kissatennana_transaksi_penjualan__level_product') }}