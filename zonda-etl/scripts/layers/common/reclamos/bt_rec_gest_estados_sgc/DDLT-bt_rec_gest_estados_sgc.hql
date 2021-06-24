 CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_rec_gest_estados_sgc (
   cod_rec_entidad string
  ,cod_rec_sector string
  ,cod_rec_gestion_nro int
  ,cod_rec_estado_gest int
  ,ts_rec_estado_gest timestamp
  ,cod_rec_tipo_medio int
  ,ds_rec_tipo_medio string
  ,ds_rec_detall_tipo_medio string
  ,cod_rec_indi_tipo_medio string
  ,cod_rec_est_medio string
  ,ds_rec_sector_owner_medio string
  ,flag_rec_visible_medio int
  ,cod_rec_mjs_asoc_medio string
  ,cod_rec_resol_predef int
  ,cod_rec_contc int
  ,cod_rec_niv_conf int
  ,flag_rec_impre_masiva int
  ,fc_rec_imp_gest_estado decimal(13,2)
  ,cod_rec_mone_imp string
  ,cod_rec_carta int
  ,cod_rec_sect_asign_espec string
  ,ts_rec_imp_gest_estado timestamp
  ,cod_rec_user_estado string
  ,cod_rec_sector_estado string
  ,ds_rec_estado string
  ,int_rec_orden_estado int
  ,cod_rec_responsab_est int
  ,int_rec_actor_nro_orden int
  ,ds_rec_comentario string
  ,cod_rec_ide_gestion_sec int
  ,flag_rec_modf_fec_resol int
  ,ds_rec_sector string
  ,cod_rec_grupo_sector string
  ,cod_rec_actral_sector string
  ,cod_rec_pcia_sector int
  ,ds_rec_mail_sector string
  ,cod_rec_estado_sector string
  ,flag_rec_enviar_mail_sector int
  ,flag_rec_info_asig_esp_sector int
  ,cod_rec_pais_sector string
  ,flag_rec_resol_info_adj_sector int
  ,cod_rec_ctro_costos_sector string
  ,flag_rec_mail_bandeja_sector int
  ,flag_rec_uso_carta_sector int
  ,cod_rec_sector_owner_sector string
  ,cod_rec_grupo_empresa_sector int
  ,flag_rec_admin_sector int
  ,fc_rec_gest_x_pag_bandeja_sector int
  ,cod_suc_sucursal_sector int
  ,cod_rec_zona_sector string
  ,cod_rec_tipo_sector string
  ,ds_rec_class_pdf_sector string
  ,fc_rec_cant_gestiones_ant_sector int
  ,cod_rec_sector_gen string
  ,flag_rec_compromiso_gold_sector int
  ,flag_rec_enviar_mail_resp_sector int
  ,flag_rec_enviar_sms_resp_sector int
  ,ds_rec_sector_estado string
  ,cod_rec_grupo_sector_estado int
  ,cod_rec_actral_sector_estado string
  ,cod_rec_pcia_sector_estado int
  ,ds_rec_mail_sector_estado string
  ,cod_rec_estado_sector_estado string
  ,flag_rec_enviar_mail_sector_estado int
  ,flag_rec_info_asig_esp_sector_estado int
  ,cod_rec_pais_sector_estado string
  ,flag_rec_resol_info_adj_sector_estado int
  ,cod_rec_ctro_costos_sector_estado string
  ,flag_rec_mail_bandeja_sector_estado int
  ,flag_rec_uso_carta_sector_estado int
  ,cod_rec_sector_owner_sector_estado string
  ,cod_rec_grupo_empresa_sector_estado int
  ,flag_rec_admin_sector_estado int
  ,fc_rec_gest_x_pag_bandeja_sector_estado int
  ,cod_suc_sucursal_sector_estado int
  ,cod_rec_zona_sector_estado string
  ,cod_rec_tipo_sector_estado string
  ,ds_rec_class_pdf_sector_estado string
  ,fc_rec_cant_gestiones_ant_sector_estado int
  ,cod_rec_sector_gen_estado string
  ,flag_rec_compromiso_gold_sector_estado int
  ,flag_rec_enviar_mail_resp_sector_estado int
  ,flag_rec_enviar_sms_resp_sector_estado int
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/reclamos/bt_rec_gest_estados_sgc'