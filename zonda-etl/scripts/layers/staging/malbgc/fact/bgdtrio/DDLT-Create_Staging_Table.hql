CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.malbgc_bgdtrio(
rio_plan STRING,
  rio_concepto STRING,
  rio_tip_operacion STRING,
  rio_zona STRING,
  rio_divisa STRING,
  rio_fecha_desde STRING,
  rio_fecha_hasta STRING,
  rio_comision STRING,
  rio_entidad_umo STRING,
  rio_centro_umo STRING,
  rio_userid_umo STRING,
  rio_netname_umo STRING,
  rio_timest_umo STRING
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malbgc/fact/bgdtrio/consolidated';