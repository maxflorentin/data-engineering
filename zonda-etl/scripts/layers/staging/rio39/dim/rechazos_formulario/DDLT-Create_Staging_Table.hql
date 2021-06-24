CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.comex_rio39_rechazos_formulario (
NRO_FORM        STRING,
MOTIVO_RECHAZO  STRING,
OBSERVACIONES   STRING,
FECHA_RECHAZO   STRING,
ACTIVO          STRING
)
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio39/dim/rechazos_formulario';