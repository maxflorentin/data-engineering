CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.malbgc_bgdtplc(
bgtcplc_plan STRING,
  bgtcplc_concepto STRING,
  bgtcplc_canal STRING,
  bgtcplc_fecha_desde STRING,
  bgtcplc_comision STRING,
  bgtcplc_fecha_hasta STRING,
  bgtcplc_entidad_umo STRING,
  bgtcplc_centro_umo STRING,
  bgtcplc_userid_umo STRING,
  bgtcplc_netname_umo STRING,
  bgtcplc_timest_umo STRING
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malbgc/fact/bgdtplc/consolidated';