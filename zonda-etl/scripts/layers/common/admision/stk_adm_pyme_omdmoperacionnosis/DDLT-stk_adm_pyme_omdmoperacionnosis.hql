CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_pyme_omdmoperacionnosis (
cod_adm_tipotramite string,
cod_adm_tramite string,
ts_adm_fecproceso string,
cod_adm_tipoparticipante string,
ds_adm_variable string,
cod_adm_nosisvariable string,
cod_adm_nosisvariableac string,
cod_adm_nosisvariabledf string,
cod_adm_nosisvariablego string,
cod_adm_nosisvariableps string,
cod_adm_nosisvariablevv string,
ds_adm_json string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_pyme_omdmoperacionnosis';