DROP TABLE IF EXISTS bi_corp_common.dim_tcr_afinidad;
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_tcr_afinidad
(
    rel_aplicacion string,
    cod_tcr_afinidad   string,
    ds_tcr_afinidad    string
)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
        LINES TERMINATED BY '\n'
    STORED AS TEXTFILE
    LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/tarjetas/dim/dim_tcr_afinidad'
;