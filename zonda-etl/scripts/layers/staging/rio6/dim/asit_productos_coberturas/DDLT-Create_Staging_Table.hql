CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_asit_productos_coberturas(
    aspb_cd_producto   string,
    aspb_cd_plan       string,
    aspb_cd_cobertura  string,
    aspb_cd_detalle    string,
    aspb_nu_orden      string,
    aspb_st_detalle    string,
    aspb_nu_eventos    string,
    aspb_mt_evento     string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/dim/asit_productos_coberturas';