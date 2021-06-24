CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_adm_ind_promociones (
cod_adm_promocion string,
ds_adm_descripcion string,
ds_adm_grupo string,
flag_adm_activa int,
ts_adm_fecha_alta string,
ts_adm_fecha_baja string,
ds_adm_descripcion2 string,
ds_adm_tramite string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/dim_adm_ind_promociones'
;