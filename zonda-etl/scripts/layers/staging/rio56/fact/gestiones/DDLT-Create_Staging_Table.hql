CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio56_gestiones (
    cod_entidad  string,
    ide_gestion_sector  string,
    ide_gestion_nro  string,
    ide_circuito  string,
    indi_tipo_circ  string,
    indi_decision_comer  string,
    indi_replteo  string,
    indi_rta_inmed  string,
    indi_impre_cart  string,
    imp_autz_solicitado  string,
    cod_mone_solicitado  string,
    imp_autz_autorizado  string,
    cod_mone_autorizado  string,
    imp_autz_resolucion  string,
    cod_mone_resolucion  string,
    tpo_pers  string,
    cod_crm  string,
    comen_cliente  string,
    ide_gest_sector_relac  string,
    ide_gest_nro_relac  string,
    fec_gestion_alta  string,
    cod_bandeja_actual  string,
    fec_bandeja_actual  string,
    cod_sector_actual  string,
    inic_user_alta  string,
    indi_mail  string,
    indi_prioridad  string,
    fec_max_resol  string,
    cod_conforme  string,
    cod_user_actual  string,
    cod_grupo_empresa  string,
    cod_tipo_favorabilidad  string,
    fec_aviso_venc  string
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio56/fact/gestiones'