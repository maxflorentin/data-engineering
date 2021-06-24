CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio56_gestion_estados (
    cod_entidad  string,
    ide_gestion_sector  string,
    ide_gestion_nro  string,
    cod_est_gest  string,
    fec_est_gest  string,
    tpo_medio  string,
    cod_rta_resol_predef  string,
    cod_contc  string,
    cod_niv_conf  string,
    indi_impre_masiva  string,
    imp_gest_estado  string,
    cod_mone_imp  string,
    cod_carta  string,
    cod_sect_asign_espec  string,
    tmp_est_gest  string,
    user_estado  string,
    cod_sect_estado  string,
    cod_bandeja  string,
    orden_estado  string,
    cod_responsab_est  string,
    actor_nro_orden  string,
    comentario  string,
    ide_gestion_sec  string,
    ind_modf_fec_resol  string
        )
PARTITIONED BY (
  partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio56/dim/gestion_estados'