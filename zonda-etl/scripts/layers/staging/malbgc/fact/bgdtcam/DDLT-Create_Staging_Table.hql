CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.malbgc_bgdtcam(
cam_campania STRING,
  cam_fecha_desde STRING,
  cam_producto STRING,
  cam_subprodu STRING,
  cam_fecha_hasta STRING,
  cam_plazo INT,
  cam_fecha_vto STRING,
  cam_tarifa STRING,
  cam_period_liq STRING,
  cam_divisa STRING,
  cam_plan STRING,
  cam_descripcion STRING,
  cam_clase_liq STRING,
  cam_clase_taf STRING,
  cam_periodo_tar STRING,
  cam_indesta STRING,
  cam_fecha_estado STRING,
  cam_entidad_umo STRING,
  cam_centro_umo STRING,
  cam_userid_umo STRING,
  cam_netname_umo STRING,
  cam_timest_umo STRING
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malbgc/fact/bgdtcam/consolidated';