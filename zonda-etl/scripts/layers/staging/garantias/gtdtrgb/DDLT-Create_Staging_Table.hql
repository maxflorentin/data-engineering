CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.gtdtrgb(
  gttcrgb_rgb_cod_entidad         string,
  gttcrgb_rgb_num_garantia        int,
  gttcrgb_rgb_num_bien            int,
  gttcrgb_rgb_fec_finvali         string,
  gttcrgb_rgb_fec_inivali         string,
  gttcrgb_rgb_por_biegaran        decimal(9,6),
  gttcrgb_rgb_cod_divisa          string,
  gttcrgb_rgb_imp_limite          decimal(17,4),
  gttcrgb_rgb_cod_limite          string,
  gttcrgb_rgb_entidad_umo         string,
  gttcrgb_rgb_centro_umo          string,
  gttcrgb_rgb_userid_umo          string,
  gttcrgb_rgb_netname_umo         string,
  gttcrgb_rgb_timest_umo          string)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/garantias/fact/gtdtrgb';