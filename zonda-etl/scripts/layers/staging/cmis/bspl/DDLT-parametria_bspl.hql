DROP TABLE IF EXISTS bi_corp_staging.cmis_parametria_bspl;
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.cmis_parametria_bspl
(
    tipo                         string,
    codigo_estructural           string,
    codigo_agrupamiento_rubro    string
)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\;'
    LINES TERMINATED BY '\n'
    STORED AS TEXTFILE
    LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/cmis/parametria_bspl';