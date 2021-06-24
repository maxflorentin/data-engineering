CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.comex_rio39_interfaces_tipo_mov(
TIPO_MOV			STRING,
DESCRIPCION         STRING,
TIPO_TRANSACCION    STRING,
INTERFACE           STRING,
CUSTOMER_RPT        STRING,
INT_PCIOS_TRANSF    STRING
)
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio39/dim/interfaces_tipo_mov';