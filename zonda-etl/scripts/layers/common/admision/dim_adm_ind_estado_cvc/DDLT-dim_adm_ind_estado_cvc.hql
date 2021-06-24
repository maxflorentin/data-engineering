CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_adm_ind_estado_cvc (
cod_adm_estado int,
ds_adm_estado string,
cod_adm_sector string,
cod_adm_estadoalcen string,
flag_adm_maregresadofinal string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '/santander/bi-corp/common/admision/dim_adm_ind_estado_cvc';