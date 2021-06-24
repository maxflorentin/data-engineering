CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio15_nb_desc_trx
(
transaccion         STRING,
descripcion         STRING,
modulo              STRING,
tipo_trx            STRING,
submodulo           STRING
)
PARTITIONED BY (
  partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio15/dim/nb_desc_trx/'