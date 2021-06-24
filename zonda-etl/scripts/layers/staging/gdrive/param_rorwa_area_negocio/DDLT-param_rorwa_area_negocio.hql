CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.param_rorwa_area_negocio(
    cod_area_negocio	string,
    area_negocio	string,
    cod_adn_cg		string)
PARTITIONED BY (partition_date string)
STORED AS PARQUET
LOCATION
'${DATA_LAKE_SERVER}/santander/bi-corp/staging/gdrive/param_rorwa_area_negocio';