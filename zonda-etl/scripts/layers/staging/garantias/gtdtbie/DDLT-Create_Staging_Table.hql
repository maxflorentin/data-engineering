CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.gtdtbie(
  gttcbie_bie_cod_entidad  string,
  gttcbie_bie_num_bien     int,
  gttcbie_bie_cod_bien     string,
  gttcbie_bie_fec_alta     string,
  gttcbie_bie_fec_vcto     string,
  gttcbie_bie_des_bien     string,
  gttcbie_bie_des_ubicfisi string,
  gttcbie_bie_cod_oficina  string,
  gttcbie_bie_cod_estado   string,
  gttcbie_bie_tip_datoscom string,
  gttcbie_bie_ind_anticres string,
  gttcbie_bie_entidad_umo  string,
  gttcbie_bie_centro_umo   string,
  gttcbie_bie_userid_umo   string,
  gttcbie_bie_netname_umo  string,
  gttcbie_bie_timest_umo   string)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/garantias/fact/gtdtbie';