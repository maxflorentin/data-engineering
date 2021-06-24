CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio78_dispositivo(
  ID_TERMINAL string,
  TOKEN string,
  ID_RUNTIME string,
  DEVICE_ID string,
  DEVICE_MODEL string,
  FECHA string,
  DEVICE_HARDWARE_ID string,
  DEVICE_MAC_ADDRESS string,
  DEVICE_VERSION_SO string,
  DEVICE_OS_ID string)
PARTITIONED BY (
  partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio78/fact/dispositivo';
