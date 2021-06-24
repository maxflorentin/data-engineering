CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio56_sectores (
    cod_entidad  string,
    cod_sector  string,
    desc_sect  string,
    cod_grupo  string,
    indi_actral  string,
    cod_pcia  string,
    mail_sector  string,
    est_sect  string,
    user_alt_sect  string,
    fec_alt_sect  string,
    user_modf_sect  string,
    fec_modf_sect  string,
    enviar_mail  string,
    indi_info_asig_esp  string,
    cod_pais  string,
    indi_resol_info_adj  string,
    cod_ctro_costos  string,
    indi_mail_bandeja  string,
    indi_uso_carta  string,
    sector_owner  string,
    cod_grupo_empresa  string,
    indi_admin  string,
    gest_x_pag_bandeja  string,
    cod_sucursal  string,
    cod_zona  string,
    tipo_sector  string,
    class_pdf  string,
    dias_gestiones_ant  string,
    cod_sector_gen  string,
    indi_compromiso_gold  string,
    indi_enviar_mail_resp  string,
    indi_enviar_sms_resp  string,
    indi_no_enviar_mya  string,
    indi_instancias_post  string
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio56/dim/sectores'