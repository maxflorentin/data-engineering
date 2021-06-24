CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.gtdtrgc(
  gttcrgc_rgc_cod_entidad string,
  gttcrgc_rgc_num_garantia int,
  gttcrgc_rgc_cod_relacion string,
  gttcrgc_rgc_num_persona string,
  gttcrgc_rgc_fec_finvali string,
  gttcrgc_rgc_fec_inivali string,
  gttcrgc_rgc_num_secclien int,
  gttcrgc_rgc_entidad_umo string,
  gttcrgc_rgc_centro_umo string,
  gttcrgc_rgc_userid_umo string,
  gttcrgc_rgc_netname_umo string,
  gttcrgc_rgc_timest_umo string)
PARTITIONED BY (
  partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/garantias/fact/gtdtrgc'
