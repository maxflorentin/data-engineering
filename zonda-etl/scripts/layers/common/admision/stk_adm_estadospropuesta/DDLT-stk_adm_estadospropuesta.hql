CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_estadospropuesta (
cod_adm_tramite string,
cod_adm_nro_prop int,
dt_adm_fec_log string,
ds_adm_log string,
ds_adm_obs_log string,
cod_adm_usuario string,
ds_adm_estado_prop string,
cod_adm_estado_accion string,
cod_adm_estado_origen string,
ds_adm_sector_estado string,
ds_adm_tipo_estado string,
ds_adm_origen string,
cod_adm_penumper string,
int_adm_cuit_cliente bigint,
cod_adm_campania string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_estadospropuesta';