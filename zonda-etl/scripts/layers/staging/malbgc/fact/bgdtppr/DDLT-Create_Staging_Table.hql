CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.malbgc_bgdtppr(
  ppr_plan STRING,
  ppr_concepto STRING,
  ppr_pco_ecu_per STRING,
  ppr_pco_ecu_sop STRING,
  ppr_fecha_desde STRING,
  ppr_fecha_hasta STRING,
  ppr_comision STRING,
  ppr_entidad_umo STRING,
  ppr_centro_umo STRING,
  ppr_userid_umo STRING,
  ppr_netname_umo STRING,
  ppr_timest_umo STRING
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malbgc/fact/bgdtppr/consolidated';