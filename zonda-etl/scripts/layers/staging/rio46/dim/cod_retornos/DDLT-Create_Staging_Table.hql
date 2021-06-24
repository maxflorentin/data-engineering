CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio46_cod_retornos (
ID_COD_RETORNO      STRING,
COD_RETORNO_GENESYS STRING,
COD_RETORNO_CACS    STRING,
DESCRIP             STRING
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio46/dim/cod_retornos'