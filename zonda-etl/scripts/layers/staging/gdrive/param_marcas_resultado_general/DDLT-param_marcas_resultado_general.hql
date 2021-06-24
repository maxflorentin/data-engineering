CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.param_marcas_resultado_general(
    cod_nivel	string,
    cod_nodo	string,
    cod_marca	string,
    desc_marca	string,
    dt_carga string)
STORED AS PARQUET
LOCATION
'${DATA_LAKE_SERVER}/santander/bi-corp/staging/gdrive/param_marcas_resultado_general';