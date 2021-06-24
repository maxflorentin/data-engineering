CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.gtdtvab(
  gttcvab_vab_cod_entidad         string,
  gttcvab_vab_num_bien            int,
  gttcvab_vab_tip_valor           string,
  gttcvab_vab_fec_alta            string,
  gttcvab_vab_cod_divisa          string,
  gttcvab_vab_imp_total           decimal(17,4),
  gttcvab_vab_entidad_umo         string,
  gttcvab_vab_centro_umo          string,
  gttcvab_vab_userid_umo          string,
  gttcvab_vab_netname_umo         string,
  gttcvab_vab_timest_umo          string)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/garantias/fact/gtdtvab'