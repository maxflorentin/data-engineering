CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_ind_omdmoferta (
cod_adm_tipotramite string,
cod_adm_tramite string,
ts_adm_fecproceso string,
ds_adm_variable string,
fc_adm_asistenciamaximacliente double,
cod_adm_codpromocion string,
cod_adm_codigoamex string,
cod_adm_codigomaster string,
cod_adm_codigovisa string,
cod_adm_codigovisabusiness string,
fc_adm_disponibleacc double,
fc_adm_disponibleamex double,
fc_adm_disponiblemaster double,
fc_adm_disponibleppp double,
fc_adm_disponiblesc1 double,
fc_adm_disponiblevisa double,
fc_adm_disponiblevisabusiness double,
fc_adm_limitemaxacc double,
fc_adm_limitemaxamex double,
fc_adm_limitemaxmaster double,
fc_adm_limitemaxppp double,
fc_adm_limitemaxsc1 double,
fc_adm_limitemaxvisa double,
fc_adm_limitemaxvisabusiness double,
fc_adm_tomadoacc double,
fc_adm_tomadoamex double,
fc_adm_tomadomaster double,
fc_adm_tomadoppp double,
fc_adm_tomadosc1 double,
fc_adm_tomadovisa double,
fc_adm_tomadovisabusiness double,
cod_adm_codpaquete string,
ds_adm_json string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_ind_omdmoferta'
;