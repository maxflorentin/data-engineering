DROP TABLE IF EXISTS bi_corp_common.dim_tcr_producto;
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_tcr_producto
(
    cod_tcr_producto      string,
    ds_tcr_producto       string,
    ds_tcr_producto_banco string,
    ds_tcr_producto_agrup string,
    cod_tcr_orden         int
)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY '\t'
        LINES TERMINATED BY '\n'
    STORED AS TEXTFILE
    LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/tarjetas/dim/dim_tcr_producto';