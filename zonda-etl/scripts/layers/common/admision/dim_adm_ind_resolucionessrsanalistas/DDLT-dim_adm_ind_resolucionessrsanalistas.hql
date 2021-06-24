CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_adm_ind_resolucionessrsanalistas (
cod_adm_legajo string,
ds_adm_usuario_srs string,
ds_adm_usuario string,
ds_adm_rol string,
ds_adm_equipo int,
flag_adm_activo int,
ts_adm_fechaalta string,
ts_adm_fechabaja string,
ds_adm_seniority string,
flag_adm_cpi int
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/dim_adm_ind_resolucionessrsanalistas'
;