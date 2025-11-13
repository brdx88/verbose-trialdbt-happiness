SELECT
    CAST(lokasi AS STRING) AS lokasi,
    CAST(no_tanda_terima AS STRING) AS no_tanda_terima,
    CAST(no_penjualan AS STRING) AS no_penjualan,
    CAST(tanggal_penjualan AS DATE) AS tanggal_penjualan,
    CAST(waktu_penjualan AS STRING) AS waktu_penjualan,
    CAST(UPPER(tipe_pesanan) AS STRING) AS tipe_pesanan,
    CAST(jumlah_bersih AS BIGINT) AS jumlah_bersih,
    CAST(biaya_layanan AS BIGINT) AS biaya_layanan,
    CAST(total_pajak AS BIGINT) AS total_pajak,
    CAST(pembulatan AS BIGINT) AS pembulatan,
    CAST(total_penjualan AS BIGINT) AS total_penjualan 
FROM {{ source('banking_bronze', 'kissatennana_transaksi_penjualan__level_receiptno') }}
