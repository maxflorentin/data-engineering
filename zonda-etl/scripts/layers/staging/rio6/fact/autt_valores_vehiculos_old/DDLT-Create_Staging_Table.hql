CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_autt_valores_vehiculos
(
AUVV_AUMA_CD_MARCA                  STRING,
AUVV_AUMO_CD_MODELO                 STRING,
AUVV_VERSION                        STRING,
AUVV_ANO                            STRING,
AUVV_VALOR                          STRING

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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/fact/autt_valores_vehiculos/'