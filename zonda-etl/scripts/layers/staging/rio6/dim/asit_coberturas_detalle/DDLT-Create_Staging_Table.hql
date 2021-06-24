CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_asit_coberturas_detalle(
    ascd_cd_cobertura   string,
    ascd_cd_detalle   string,
    ascd_de_detalle   string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/dim/asit_coberturas_detalle';