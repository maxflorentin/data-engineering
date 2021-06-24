CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.malbgc_bgdtban(
  ban_plan STRING,
  ban_concepto STRING,
  ban_titularidad STRING,
  ban_red STRING,
  ban_fecha_desde STRING,
  ban_fecha_hasta STRING,
  ban_comision STRING,
  ban_entidad_umo STRING,
  ban_centro_umo STRING,
  ban_userid_umo STRING,
  ban_netname_umo STRING,
  ban_timest_umo STRING
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malbgc/fact/bgdtban/consolidated';