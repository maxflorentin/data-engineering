CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.malbgc_bgdtpab(
pab_num_convenio INT,
  pab_concepto STRING,
  pab_porc_suscriptor DECIMAL(8,5),
  pab_porc_entidad DECIMAL(8,5),
  pab_porc_cliente DECIMAL(8,5),
  pab_entidad STRING,
  pab_centro_alta STRING,
  pab_cuenta STRING,
  pab_divisa STRING,
  pab_indesta STRING,
  pab_fecha_est STRING,
  pab_entidad_umo STRING,
  pab_centro_umo STRING,
  pab_userid_umo STRING,
  pab_netname_umo STRING,
  pab_timest_umo STRING
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malbgc/fact/bgdtpab/consolidated';