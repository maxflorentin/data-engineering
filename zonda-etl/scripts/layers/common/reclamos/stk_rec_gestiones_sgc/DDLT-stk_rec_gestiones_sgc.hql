CREATE EXTERNAL TABLE bi_corp_common.stk_rec_gestiones_sgc (
  cod_rec_entidad STRING,
  cod_rec_sector STRING,
  cod_rec_gestion_nro INT,
  cod_per_nup INT,
  cod_rec_circuito INT,
  cod_rec_tipo_circuito STRING,
  flag_rec_decision_comer INT,
  flag_rec_replteo INT,
  flag_rec_rta_inmed INT,
  flag_rec_impre_cart INT,
  fc_rec_imp_autz_solicitado DECIMAL(13,2),
  cod_rec_mone_solicitado STRING,
  fc_rec_imp_autz_autorizado DECIMAL(13,2),
  cod_rec_mone_autorizado STRING,
  fc_rec_imp_autz_resolucion DECIMAL(13,2),
  cod_rec_mone_resolucion STRING,
  cod_rec_tipo_persona STRING,
  cod_rec_crm STRING,
  ds_rec_comentario_cliente STRING,
  cod_rec_gestion_sector_relac STRING,
  cod_rec_gestion_nro_relac INT,
  ts_rec_gestion_alta TIMESTAMP,
  ds_rec_bandeja_actual STRING,
  ts_rec_bandeja_actual TIMESTAMP,
  cod_rec_sector_actual STRING,
  cod_rec_inic_user_alta STRING,
  cod_rec_mail STRING,
  cod_rec_prioridad INT,
  ts_rec_max_resol TIMESTAMP,
  cod_rec_conforme STRING,
  cod_rec_user_actual STRING,
  cog_rec_grupo_empresa INT,
  cod_rec_tipo_favorabilidad INT,
  dt_rec_aviso_vencimiento STRING,
  cod_rec_circuito_secundario INT,
  dt_rec_vig_desde_circuito STRING,
  dt_rec_vig_hasta_circuito STRING,
  ds_rec_circuito STRING,
  ds_rec_detalle_circuito STRING,
  cod_rec_tipo_gestion INT,
  ds_rec_tipo_gestion STRING,
  ds_rec_detallet_tipo_gestion STRING,
  cod_rec_producto INT,
  ds_rec_produto STRING,
  cod_rec_estado_producto STRING,
  cod_rec_subproducto INT,
  ds_rec_subproducto STRING,
  cod_rec_estado_subproducto STRING,
  cod_rec_concepto INT,
  ds_rec_concepto STRING,
  ds_rec_detalle_concepto STRING,
  cod_rec_estado_concepto STRING,
  ds_rec_concepto_sac STRING,
  cod_rec_tipo_reclamo_sac INT,
  cod_rec_estado_concepto_sac STRING,
  ds_rec_concepto_bcra STRING,
  cod_rec_tipo_reclamo_bcra INT,
  cod_rec_estado_concepto_bcra STRING,
  ds_rec_concepto_espana STRING,
  cod_rec_tipo_reclamo_espana INT,
  cod_rec_estado_concepto_espana STRING,
  cod_rec_subconcepto_circuito INT,
  int_rec_tmp_asign_especial_circuito INT,
  int_rec_tmp_pedido_info_circuito INT,
  int_rec_tmp_circuito INT,
  flag_rec_gestion_pendiente_circuito INT,
  flag_rec_mail_demora_circuito INT,
  cod_rec_cierre_automatico_circuito STRING,
  flag_rec_autoriza_item_circuito INT,
  cod_rec_recibo_circuito INT,
  cod_rec_estado_circuito STRING,
  cod_rec_usuario_alta_circuito STRING,
  ts_rec_alta_circuito TIMESTAMP,
  cod_rec_usuario_modif_circuito STRING,
  ts_rec_modif_circuito TIMESTAMP,
  cod_rec_modif_datos_circuito STRING,
  flag_rec_recep_resp_circuito INT,
  flag_rec_sucursales_circuito INT,
  flag_rec_dejar_pend_circuito INT,
  cod_rec_tipo_resolucion_circuito STRING,
  cod_rec_und_tiempo_circuito STRING,
  cod_rec_tipo_vist_gest_circuito STRING,
  cod_rec_suma_tmp_autz_circuito STRING,
  cod_rec_conforme_circuito INT,
  flag_rec_jerarquia_autz_circuito INT,
  flag_rec_secuencia_autz_circuito INT,
  flag_rec_asig_paralelo_circuito INT,
  flag_rec_autoriz_devol_circuito INT,
  flag_rec_modf_fec_circuito INT,
  cod_rec_circuito_predecesor_circuito INT,
  flag_rec_crit_circuito INT,
  flag_rec_info_multivaluada_circuito INT,
  flag_rec_enviar_mail_recep_circuito INT,
  flag_rec_enviar_sms_recep_circuito INT,
  flag_rec_enviar_mail_resp_circuito INT,
  flag_rec_enviar_sms_resp_circuito INT,
  flag_rec_enviar_mail_demora_circuito INT,
  cod_rec_modelo_msj_circuito INT,
  cod_rec_clasif_select_circuito INT,
  cod_rec_parrafo_cuerpo_mail_circuito INT,
  flag_rec_uso_monto_circuito INT,
  flag_rec_indi_enviar_carta_resp_circuito INT,
  cod_rec_comprobante_cliente_circuito INT,
  flag_rec_enviar_mail_prov_circuito INT,
  cod_rec_reapertura_circuito STRING,
  cod_rec_long_comentario_recep_circuito INT,
  cod_rec_long_comentario_cliente_circuito INT,
  int_rec_aviso_ven_gest_circuito INT,
  flag_rec_enviar_mail_no_autz_circuito INT,
  cod_rec_entidad_afect_circuito INT,
  ds_rec_sector STRING,
  cod_rec_grupo_sector STRING,
  cod_rec_actral_sector STRING,
  cod_rec_pcia_sector INT,
  ds_rec_mail_sector STRING,
  cod_rec_estado_sector STRING,
  flag_rec_enviar_mail_sector INT,
  flag_rec_info_asig_esp_sector INT,
  cod_rec_pais_sector STRING,
  flag_rec_resol_info_adj_sector INT,
  cod_rec_ctro_costos_sector STRING,
  flag_rec_mail_bandeja_sector INT,
  flag_rec_uso_carta_sector INT,
  cod_rec_sector_owner_sector STRING,
  cod_rec_grupo_empresa_sector INT,
  flag_rec_admin_sector INT,
  fc_rec_gest_x_pag_bandeja_sector INT,
  cod_suc_sucursal_sector INT,
  cod_rec_zona_sector STRING,
  cod_rec_tipo_sector STRING,
  ds_rec_class_pdf_sector STRING,
  fc_rec_cant_gestiones_ant_sector INT,
  cod_rec_sector_gen STRING,
  flag_rec_compromiso_gold_sector INT,
  flag_rec_enviar_mail_resp_sector INT,
  flag_rec_enviar_sms_resp_sector INT,
  ds_rec_sector_actual STRING,
  cod_rec_grupo_sector_actual STRING,
  cod_rec_actral_sector_actual STRING,
  cod_rec_pcia_sector_actual INT,
  ds_rec_mail_sector_actual STRING,
  cod_rec_estado_sector_actual STRING,
  flag_rec_enviar_mail_sector_actual INT,
  flag_rec_info_asig_esp_sector_actual INT,
  cod_rec_pais_sector_actual STRING,
  flag_rec_resol_info_adj_sector_actual INT,
  cod_rec_ctro_costos_sector_actual STRING,
  flag_rec_mail_bandeja_sector_actual INT,
  flag_rec_uso_carta_sector_actual INT,
  cod_rec_sector_owner_sector_actual STRING,
  cod_rec_grupo_empresa_sector_actual INT,
  flag_rec_admin_sector_actual INT,
  fc_rec_gest_x_pag_bandeja_sector_actual INT,
  cod_suc_sucursal_sector_actual INT,
  cod_rec_zona_sector_actual STRING,
  cod_rec_tipo_sector_actual STRING,
  ds_rec_class_pdf_sector_actual STRING,
  fc_rec_cant_gestiones_ant_sector_actual INT,
  cod_rec_sector_gen_actual STRING,
  flag_rec_compromiso_gold_sector_actual INT,
  flag_rec_enviar_mail_resp_sector_actual INT,
  flag_rec_enviar_sms_resp_sector_actual INT,
  cod_rec_nro_cta INT,
  cod_rec_nro_firm_cta INT,
  cod_rec_mone_cta STRING,
  cod_rec_prod_cta STRING,
  cod_rec_subpro_cta STRING,
  cod_tcr_tarjeta INT,
  ds_rec_link_resumen STRING,
  ds_rec_dire_mail_contc STRING,
  ds_rec_dire_mail2_contc STRING,
  int_rec_cartera INT,
  cod_rec_aplic_cta_gold STRING,
  cod_suc_sucursal_cta_gold INT,
  cod_rec_cta_gold INT,
  cod_rec_nro_firm_cta_gold INT,
  cod_rec_mone_cta_gold STRING,
  cod_rec_prod_cta_gold STRING,
  cod_rec_subpro_cta_gold STRING,
  cod_rec_ramo_cta INT,
  cod_rec_nro_certificado_cta INT,
  fc_rec_rentabilidad_promedio DECIMAL(14,2),
  cod_rec_color_semaforo STRING,
  cod_rec_color_semaf_riesgo STRING,
  cod_rec_envios_mya INT,
  ds_rec_dire_mail_opcional STRING,
  cod_rec_nro_paquete INT,
  cod_rec_paquete INT,
  flag_rec_plan_sueldo INT,
  cod_rec_nro_convenio_paquete STRING,
  ds_rec_emp_celular_contc STRING,
  cod_rec_tipo_msj_asoc_rta INT,
  fc_rec_monto_deuda_ref INT,
  flag_rec_no_enviar_notificaciones INT,
  cod_rec_id_ejecutivo STRING
)
PARTITIONED BY (partition_date string)
STORED AS PARQUET
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/reclamos/fact/stk_rec_gestiones_sgc';