

CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_cc_organigrama (

  cod_cc_empresa string,
  ds_cc_empresa string,
  cod_cc_negocio string,
  ds_cc_negocio string,
  cod_cc_jefecomercial string,
  ds_cc_jefecomercial string,
  cod_cc_jefesupervisor string,
  ds_cc_jefesupervisor string,
  ds_cc_modo string,
  ds_cc_supervisor string,
  cod_cc_grupo string,
  ds_cc_grupo string,
  cod_cc_legajo_admin string,
  cod_cc_legajo_usuariorh string,
  cod_cc_legajo_usuariont string,
  ds_cc_usuario string
)
PARTITIONED BY (
partition_date string)

STORED AS PARQUET
LOCATION
'${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/cc/fact/stk_cc_organigrama'
