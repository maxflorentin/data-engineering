CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.malbgc_tbgb001(
    001_suscriptor STRING,
  001_des_suscriptor STRING,
  001_tpo_suscriptor STRING,
  001_entidad STRING,
  001_centro_alta STRING,
  001_cuenta STRING,
  001_divisa STRING,
  001_entidad_umo STRING,
  001_centro_umo STRING,
  001_userid_umo STRING,
  001_netname_umo STRING,
  001_timest_umo STRING
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malbgc/fact/tbgb001/consolidated';