CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.malbgc_bgdtcpc(
cpc_concepto STRING,
  cpc_comision STRING,
  cpc_canal STRING,
  cpc_fecha_desde STRING,
  cpc_fecha_hasta STRING,
  cpc_entidad_umo STRING,
  cpc_centro_umo STRING,
  cpc_userid_umo STRING,
  cpc_netname_umo STRING,
  cpc_timest_umo STRING
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malbgc/fact/bgdtcpc/consolidated';