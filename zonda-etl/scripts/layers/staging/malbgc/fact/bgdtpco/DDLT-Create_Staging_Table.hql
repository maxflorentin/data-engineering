CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.malbgc_bgdtpco(
    pco_plan	string,
    pco_concepto	string,
    pco_fecha_desde	string,
    pco_fecha_hasta	string,
    pco_mvtos_libr_riom	int,
    pco_period_com	string,
    pco_period_cobr	string,
    pco_dia_nat_cobr	int,
    pco_entidad_umo	string,
    pco_centro_umo	string,
    pco_userid_umo	string,
    pco_netname_umo	string,
    pco_timest_umo	string
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malbgc/fact/bgdtpco/consolidated';