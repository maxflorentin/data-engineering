CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_ind_omdmafip (
cod_adm_tipotramite string,
cod_adm_tramite string,
ts_adm_fecproceso string,
cod_adm_tipoparticipante string,
cod_adm_actmonotributo string,
flag_adm_baseafipmono string,
cod_adm_empleador string,
cod_adm_impganancia string,
cod_adm_impiva string,
cod_adm_integrantesoc string,
cod_adm_monotributo string,
ds_adm_json string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_ind_omdmafip'
;