CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.param_marcas_area_negocio(
    cod_adn_n1 string,
    ds_adn_n1 string,
    cod_adn_n2 string,
    ds_adn_n2 string,
    cod_adn_n3 string,
    ds_adn_n3 string,
    cod_adn_n4 string,
    ds_adn_n4 string,
    nodo string,
    nivel string,
    dt_carga string)
STORED AS PARQUET
LOCATION
'${DATA_LAKE_SERVER}/santander/bi-corp/staging/gdrive/param_marcas_area_negocio';
