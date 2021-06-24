CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.malbgc_bgdtobs(
obs_entidad string,
obs_centro_alta string,
obs_cuenta string,
obs_divisa string,
obs_numer_mov int,
obs_observaciones string,
obs_entidad_umo string,
obs_centro_umo string,
obs_userid_umo string,
obs_cajero_umo string,
obs_netname_umo string,
obs_timest_umo string
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malbgc/fact/bgdtobs'