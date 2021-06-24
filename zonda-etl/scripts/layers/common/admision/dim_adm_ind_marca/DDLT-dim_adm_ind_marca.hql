CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_adm_ind_marca (
cod_adm_marca int,
ds_adm_marca string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/dim_adm_ind_marca';