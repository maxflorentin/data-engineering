CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio147_hb_desc_trx(
TRANSACCION                         STRING,
DESCRIPCION                         STRING,
MODULO                              STRING,
TIPO_TRX                            STRING
)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio147/dim/hb_desc_trx';