CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_adm_pyme_estadosoperacion (
cod_adm_tramite string,
cod_adm_estado string,
cod_adm_estado_accion string,
cod_adm_estado_origen string,
ds_adm_des_estado string,
ds_adm_sge string,
ds_adm_srs string,
ds_adm_datos_adic string,
flag_adm_mar_contador string,
flag_adm_mar_pisa_paq string,
int_adm_cant_dias int,
int_adm_cant_dia int,
ds_adm_estado string,
ds_adm_sector string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/dim_adm_pyme_estadosoperacion';