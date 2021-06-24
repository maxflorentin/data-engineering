CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.comex_rio39_opp_productos (
COD_PROD        STRING,
DESC_PROD       STRING,
LIABILITY_BKT   STRING,
HAB_CANALES     STRING
)
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio39/dim/opp_productos';