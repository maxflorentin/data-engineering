CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.gtdtmae(
  gttcmae_mae_cod_entidad string,
  gttcmae_mae_num_garantia int,
  gttcmae_mae_cod_garantia string,
  gttcmae_mae_tip_cobertur string,
  gttcmae_mae_fec_alta string,
  gttcmae_mae_fec_vcto string,
  gttcmae_mae_fec_activa string,
  gttcmae_mae_cod_oficina string,
  gttcmae_mae_cod_divisa string,
  gttcmae_mae_imp_limite decimal(17,4),
  gttcmae_mae_fec_camlim string,
  gttcmae_mae_ind_bancosor string,
  gttcmae_mae_cod_estado string,
  gttcmae_mae_cod_subestad string,
  gttcmae_mae_imp_disponib decimal(17,4),
  gttcmae_mae_imp_alzado decimal(17,4),
  gttcmae_mae_fec_ultcober string,
  gttcmae_mae_des_ubicarpe string,
  gttcmae_mae_ind_reconsti string,
  gttcmae_mae_cod_administ string,
  gttcmae_mae_cod_unidgest string,
  gttcmae_mae_imp_contable decimal(17,4),
  gttcmae_mae_imp_nominal decimal(17,4),
  gttcmae_mae_entidad_umo string,
  gttcmae_mae_centro_umo string,
  gttcmae_mae_userid_umo string,
  gttcmae_mae_netname_umo string,
  gttcmae_mae_timest_umo string)
PARTITIONED BY (
  partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/garantias/fact/gtdtmae';