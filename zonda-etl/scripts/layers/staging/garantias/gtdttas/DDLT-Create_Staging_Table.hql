CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.gtdttas(
    gttctas_tas_cod_entidad string,
    gttctas_tas_num_bien int,
    gttctas_tas_fec_tasacion string,
    gttctas_tas_num_persona string,
    gttctas_tas_fec_protasac string,
    gttctas_tas_hor_fintasac string,
    gttctas_tas_des_observac string,
    gttctas_tas_fec_proinspe string,
    gttctas_tas_fec_prosuper string,
    gttctas_tas_ind_tasinsup string,
    gttctas_tas_ind_agropec string,
    gttctas_tas_entidad_umo string,
    gttctas_tas_centro_umo string,
    gttctas_tas_userid_umo string,
    gttctas_tas_netname_umo string,
    gttctas_tas_timest_umo string)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/garantias/fact/gtdttas';