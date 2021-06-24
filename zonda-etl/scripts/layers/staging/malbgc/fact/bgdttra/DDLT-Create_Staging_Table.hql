CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.malbgc_bgdttra(
tra_concepto STRING,
  tra_comision STRING,
  tra_fecha_desde STRING,
  tra_saldo_hasta DECIMAL(17,4),
  tra_fecha_hasta STRING,
  tra_comi_im DECIMAL(17,4),
  tra_comi_po DECIMAL(8,5),
  tra_comi_max DECIMAL(17,4),
  tra_comi_min DECIMAL(17,4),
  tra_entidad_umo STRING,
  tra_centro_umo STRING,
  tra_userid_umo STRING,
  tra_netname_umo STRING,
  tra_timest_umo STRING
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malbgc/fact/bgdttra/consolidated';