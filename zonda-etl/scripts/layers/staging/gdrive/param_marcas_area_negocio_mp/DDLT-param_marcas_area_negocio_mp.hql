CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.param_marcas_area_negocio_mp(
    cod_ren_area_negocio string,
    dt_carga string)
STORED AS PARQUET
LOCATION
'${DATA_LAKE_SERVER}/santander/bi-corp/staging/gdrive/param_marcas_area_negocio_mp';
