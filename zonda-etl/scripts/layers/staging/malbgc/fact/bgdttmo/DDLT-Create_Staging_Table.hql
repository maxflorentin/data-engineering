CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.malbgc_bgdttmo(
  tmo_concepto STRING,
  tmo_comision STRING,
  tmo_fecha_desde STRING,
  tmo_codigo STRING,
  tmo_fecha_hasta STRING,
  tmo_entidad_umo STRING,
  tmo_centro_umo STRING,
  tmo_userid_umo STRING,
  tmo_netname_umo STRING,
  tmo_timest_umo STRING
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malbgc/fact/bgdttmo/consolidated';