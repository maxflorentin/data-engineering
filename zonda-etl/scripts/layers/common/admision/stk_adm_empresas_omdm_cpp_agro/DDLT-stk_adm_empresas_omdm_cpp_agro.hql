CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_empresas_omdm_cpp_agro (
    cod_adm_tipotramite string,
    cod_adm_tramite string,
    ts_adm_fecproceso string,
    cod_adm_tipoparticipante string,
    int_adm_agrocantcria int,
    int_adm_agrocantinvernada int,
    fc_adm_agrogastosestructura double,
    int_adm_agroprodltleche int,
    ds_adm_json string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_empresas_omdm_cpp_agro';