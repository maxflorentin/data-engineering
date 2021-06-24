CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.gtdtrbc(
  gttcrbc_rbc_cod_entidad         string,
  gttcrbc_rbc_num_bien            int,
  gttcrbc_rbc_cod_relacion        string,
  gttcrbc_rbc_num_persona         string,
  gttcrbc_rbc_fec_finvali         string,
  gttcrbc_rbc_fec_inivali         string,
  gttcrbc_rbc_num_secclien        int,
  gttcrbc_rbc_entidad_umo         string,
  gttcrbc_rbc_centro_umo          string,
  gttcrbc_rbc_userid_umo          string,
  gttcrbc_rbc_netname_umo         string,
  gttcrbc_rbc_timest_umo          string)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/garantias/fact/gtdtrbc';