CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio78_transaccion(
  TRANSACCION string,
  DESCRIPCION string,
  MODULO string,
  HABILITADO string)
PARTITIONED BY (
  partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio78/dim/transaccion';
