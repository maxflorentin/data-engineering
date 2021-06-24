CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_adm_empresas_omdm_cpp_invernada (
    cod_adm_tipotramite string,
    cod_adm_tramite string,
    ts_adm_fecproceso string,
    fc_Adm_totalCompras double,
    fc_adm_totalCostoDirecto double,
    fc_adm_totalFacturacionBruta double,
    fc_adm_totalGastosComercializacion double,
    ds_adm_json string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/stk_adm_empresas_omdm_cpp_invernada';