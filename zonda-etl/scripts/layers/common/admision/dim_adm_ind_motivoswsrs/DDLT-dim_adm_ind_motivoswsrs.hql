CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_adm_ind_motivoswsrs (
cod_adm_grupodecision string,
cod_adm_motivo string,
fc_adm_nroprioridad int,
cod_adm_motivoasol string,
ds_adm_nomaplicativo string,
ds_adm_motivo string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/dim_adm_ind_motivoswsrs';