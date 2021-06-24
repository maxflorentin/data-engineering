CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio56_circuito (
    cod_entidad  string,
    ide_circuito  string,
    cod_circuito  string,
    fec_vig_dde_circ  string,
    fec_vig_hta_circ  string,
    desc_circ  string,
    desc_detall_circ  string,
    tpo_gestion  string,
    cod_prod  string,
    cod_subprod  string,
    cod_cpto  string,
    cod_subcpto  string,
    tmp_asign_especial  string,
    tmp_pedido_info  string,
    tmp_circ  string,
    indi_gest_pend  string,
    indi_mail_demora  string,
    indi_cierr_autm  string,
    indi_autoriza_item  string,
    cod_recibo  string,
    est_circ  string,
    user_alt_circ  string,
    fec_alt_circ  string,
    user_modf_circ  string,
    fec_modf_circ  string,
    indi_modif_datos  string,
    indi_recep_resp  string,
    indi_sucursales  string,
    indi_dejar_pend  string,
    cod_tpo_resol  string,
    und_tiempo  string,
    tpo_vis_gest  string,
    indi_suma_tmp_autz  string,
    cod_conforme  string,
    indi_jerarquia_autz  string,
    indi_secuencia_autz  string,
    indi_asig_paralelo  string,
    indi_autoriz_devol  string,
    indi_modf_fec  string,
    circuito_predecesor  string,
    indi_crit  string,
    indi_info_multivaluada  string,
    indi_enviar_mail_recep  string,
    indi_enviar_sms_recep  string,
    indi_enviar_mail_resp  string,
    indi_enviar_sms_resp  string,
    indi_enviar_mail_demora  string,
    cod_modelo_msj  string,
    id_clasif_select  string,
    id_parrafo_cuerpo_mail  string,
    indi_uso_monto  string,
    indi_enviar_carta_resp  string,
    cod_comprobante_cliente  string,
    indi_enviar_mail_prov  string,
    indi_reapertura  string,
    long_comentario_recep  string,
    long_comentario_cliente  string,
    aviso_venc_gest  string,
    indi_enviar_mail_no_autz  string,
    cod_entidad_afect  string
        )
PARTITIONED BY (
  partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
WITH SERDEPROPERTIES ('field.delim'='^')
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio56/dim/circuito'