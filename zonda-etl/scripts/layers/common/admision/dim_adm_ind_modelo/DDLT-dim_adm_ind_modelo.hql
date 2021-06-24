CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_adm_ind_modelo (
cod_adm_modelo int,
ds_adm_modelo string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '/santander/bi-corp/common/admision/dim_adm_ind_modelo';