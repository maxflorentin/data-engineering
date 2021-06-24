CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_pyme_accionistas_prop (
cod_adm_nro_prop int,
int_adm_secuencia int,
ds_adm_nombre string,
dec_adm_porcentaje double,
ds_adm_cargo string,
dec_adm_solvencia double,
ds_adm_cliente string,
cod_adm_peusualt string,
cod_adm_peusumod string,
dt_adm_pefecalt string,
dt_adm_pefecmod string,
cod_adm_penumper int,
cod_per_nup int,
cod_adm_cuit bigint,
ds_adm_tipo_doc string,
cod_adm_nro_doc int,
int_adm_nro_orden int,
flag_adm_mar_sexo string,
dt_adm_fec_nac string,
flag_adm_mar_error_altair string,
ds_adm_msj_error_altair string,
ds_adm_apellido string,
ds_adm_forma_juridica string,
ds_adm_tipo_doc_conyuge string,
int_adm_nro_doc_conyuge int,
cod_adm_estado_civil string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_pyme_accionistas_prop';