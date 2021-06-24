CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.comex_rio39_nup_por_celula (
ID_CELULA       STRING,
NUP             STRING,
USU_MODIF       STRING,
FECHA_MODIF     STRING,
RAZON_SOCIAL    STRING,
TIPO_DOC        STRING,
NRO_DOC         STRING
)
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio39/dim/nup_por_celula';