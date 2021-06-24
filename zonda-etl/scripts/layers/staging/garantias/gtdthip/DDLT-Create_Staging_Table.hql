CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.gtdthip(
  gttchip_hip_cod_entidad         string,
  gttchip_hip_num_bien            int,
  gttchip_hip_tip_bienraiz        string,
  gttchip_hip_num_rolprop         string,
  gttchip_hip_num_metrtot         decimal(17,4),
  gttchip_hip_num_metcons         decimal(17,4),
  gttchip_hip_cod_medida          string,
  gttchip_hip_nom_callehip        string,
  gttchip_hip_num_callehip        string,
  gttchip_hip_num_pisohipo        string,
  gttchip_hip_num_departam        string,
  gttchip_hip_cod_ufhipote        string,
  gttchip_hip_des_unicompl        string,
  gttchip_hip_des_entreca1        string,
  gttchip_hip_des_entreca2        string,
  gttchip_hip_des_barriohi        string,
  gttchip_hip_cod_postal          string,
  gttchip_hip_cod_localida        string,
  gttchip_hip_des_partidoh        string,
  gttchip_hip_cod_provinci        string,
  gttchip_hip_des_circunsc        string,
  gttchip_hip_ind_ubicgeoc        string,
  gttchip_hip_des_seccionh        string,
  gttchip_hip_des_manzanah        string,
  gttchip_hip_num_parcelah        string,
  gttchip_hip_num_idenprop        string,
  gttchip_hip_tip_propieda        string,
  gttchip_hip_nom_edificio        string,
  gttchip_hip_cod_estatusa        string,
  gttchip_hip_por_bien            decimal(9,6),
  gttchip_hip_fec_ultcontr        string,
  gttchip_hip_ind_gravbien        string,
  gttchip_hip_ind_enajbien        string,
  gttchip_hip_entidad_umo         string,
  gttchip_hip_centro_umo          string,
  gttchip_hip_userid_umo          string,
  gttchip_hip_netname_umo         string,
  gttchip_hip_timest_umo          string)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/garantias/fact/gtdthip';