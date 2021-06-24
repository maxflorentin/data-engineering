CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.gtdtgar(
  gttcgar_gar_cod_entidad string,
  gttcgar_gar_cod_garantia string,
  gttcgar_gar_cla_garantia string,
  gttcgar_gar_tip_cobertur string,
  gttcgar_gar_des_larga string,
  gttcgar_gar_des_corta string,
  gttcgar_gar_fec_inicio string,
  gttcgarr_gar_fec_fin string,
  gttcgar_gar_tip_preferen string,
  gttcgar_gar_ind_constitu string,
  gttcgar_gar_cod_peralzam string,
  gttcgar_gar_num_diascomu int,
  gttcgar_gar_ind_admisible string,
  gttcgar_gar_entidad_umo string,
  gttcgar_gar_centro_umo string,
  gttcgar_gar_userid_umo string,
  gttcgar_gar_netname_umo string,
  gttcgar_gar_timest_umo string)
PARTITIONED BY (
  partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/garantias/fact/gtdtgar'
