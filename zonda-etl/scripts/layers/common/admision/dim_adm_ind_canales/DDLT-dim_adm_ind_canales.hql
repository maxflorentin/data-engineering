CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_adm_ind_canales (
cod_adm_canal string,
ds_adm_descripcion string,
ds_adm_agrupador string,
ds_adm_tipo string,
flag_adm_activo int,
ts_adm_fechaalta string,
ts_adm_fechabaja string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/dim_adm_ind_canales'
;