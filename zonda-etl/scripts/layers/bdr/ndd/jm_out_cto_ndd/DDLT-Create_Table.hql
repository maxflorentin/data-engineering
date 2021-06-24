CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_bdr.jm_out_cto_ndd(
feoperac string,
s1emp string,
contra1 string,
ind_def string,
dias_pp string,
dias_atr string,
fec_def string,
fini_pp string,
ffin_pp string,
imp_atr string,
sonbldef string,
flg_tecnico string,
finalind string
)
PARTITIONED BY (partition_date string)
STORED AS PARQUET
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/bdr/ndd/jm_out_cto_ndd';