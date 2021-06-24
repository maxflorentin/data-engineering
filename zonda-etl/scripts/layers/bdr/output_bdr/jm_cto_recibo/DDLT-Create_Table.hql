CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_cto_recibo(
t1754_feoperac string,
t1754_s1emp string,
t1754_contra1 string,
t1754_idrecibo string,
t1754_fec_orig string,
t1754_imp_rdev string,
t1754_fec_dev string
)
PARTITIONED BY (partition_date string)
STORED AS PARQUET
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/output_bdr/jm_cto_recibo';