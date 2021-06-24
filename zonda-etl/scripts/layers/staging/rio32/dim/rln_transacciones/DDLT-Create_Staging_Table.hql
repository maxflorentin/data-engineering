CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio32_rln_transacciones(
ID                                  STRING,
NOMBRE                              STRING,
IDFAMILIA                           STRING,
DESCRIPCION                         STRING,
ROBUSTECIDA                         STRING
)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio32/dim/rln_transacciones';