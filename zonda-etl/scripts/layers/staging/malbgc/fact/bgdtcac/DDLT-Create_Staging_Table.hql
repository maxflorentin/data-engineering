CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.malbgc_bgdtcac(
cac_entidad STRING,
  cac_centro_alta STRING,
  cac_cuenta STRING,
  cac_divisa STRING,
  cac_campania STRING,
  cac_fecha_desde STRING,
  cac_fecha_hasta STRING,
  cac_entidad_umo STRING,
  cac_centro_umo STRING,
  cac_userid_umo STRING,
  cac_netname_umo STRING,
  cac_timest_umo STRING
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malbgc/fact/bgdtcac/consolidated';