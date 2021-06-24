CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_cajt_categorias_iva(
    cjiv_cd_categoria_iva  string,
    cjiv_de_categoria_iva  string,
    cjiv_tp_factura        string,
    cjiv_categoria_afip    string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/dim/cajt_categorias_iva';