CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.gtdtrgo(
  gttcrgo_rgo_cod_entidad string,
  gttcrgo_rgo_num_garantia int,
  gttcrgo_rgo_fec_bajrelac string,
  gttcrgo_rgo_fec_altrelac string,
  gttcrgo_rgo_tip_cobertur string,
  gttcrgo_rgo_tip_relacion string,
  gttcrgo_rgo_ind_principa string,
  gttcrgo_rgo_imp_deuda decimal(17,4),
  gttcrgo_rgo_por_cubierto decimal(9,6),
  gttcrgo_rgo_imp_cubierto decimal(17,4),
  gttcrgo_rgo_entidad_umo string,
  gttcrgo_rgo_centro_umo string,
  gttcrgo_rgo_userid_umo string,
  gttcrgo_rgo_netname_umo string,
  gttcrgo_rgo_timest_umo string,
  gttcrgo_rgo_contrato_rgo_num_ctacont string,
  gttcrgo_rgo_contrato_rgo_cod_oficont string,
  gttcrgo_rgo_contrato_rgo_cod_entcont string,
  gttcrgo_rgo_contrato_rgo_cod_divcont string,
  gttcrgo_rgo_contrato_rgo_cod_producto string,
  gttcrgo_rgo_contrato_rgo_cod_subprod string)
PARTITIONED BY (
  partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/garantias/fact/gtdtrgo'
