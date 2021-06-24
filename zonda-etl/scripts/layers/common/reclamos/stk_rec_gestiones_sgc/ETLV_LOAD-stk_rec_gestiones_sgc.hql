set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE bi_corp_common.stk_rec_gestiones_sgc
PARTITION(partition_date)
SELECT
	trim(gestiones.cod_entidad) cod_rec_entidad ,
	trim(gestiones.ide_gestion_sector) cod_rec_sector ,
	trim(gestiones.ide_gestion_nro) cod_rec_gestion_nro ,
	coalesce(gc_individuo_gest.cod_per_nup,gc_empresa_gest.cod_per_nup) cod_per_nup,
	trim(gestiones.ide_circuito) cod_rec_circuito ,
	trim(gestiones.indi_tipo_circ) cod_rec_tipo_circuito ,
	case when gestiones.indi_decision_comer='S' then 1 else 0 end flag_rec_decision_comer ,
	case when gestiones.indi_replteo='S' then 1 else 0 end flag_rec_replteo ,
	case when gestiones.indi_rta_inmed='S' then 1 else 0 end flag_rec_rta_inmed ,
	case when gestiones.indi_impre_cart='S' then 1 else 0 end flag_rec_impre_cart ,
	trim(gestiones.imp_autz_solicitado) fc_rec_imp_autz_solicitado ,
	case when trim(gestiones.cod_mone_solicitado)='null' then null else trim(gestiones.cod_mone_solicitado) end AS cod_rec_mone_solicitado ,
	--gestiones.cod_mone_solicitado cod_rec_mone_solicitado ,
	trim(gestiones.imp_autz_autorizado) fc_rec_imp_autz_autorizado ,
	case when trim(gestiones.cod_mone_autorizado)='null' then null else trim(gestiones.cod_mone_autorizado) end AS cod_rec_mone_autorizado ,
	--gestiones.cod_mone_autorizado cod_rec_mone_autorizado ,
	trim(gestiones.imp_autz_resolucion) fc_rec_imp_autz_resolucion ,
	case when trim(gestiones.cod_mone_resolucion)='null' then null else trim(gestiones.cod_mone_resolucion) end AS cod_rec_mone_resolucion ,
	--trim(gestiones.cod_mone_resolucion) cod_rec_mone_resolucion ,
	trim(gestiones.tpo_pers) cod_rec_tipo_persona ,
	trim(gestiones.cod_crm) cod_rec_crm ,
	case when trim(gestiones.comen_cliente)='null' then null else trim(gestiones.comen_cliente) end AS ds_rec_comentario_cliente ,
	case when trim(gestiones.ide_gest_sector_relac)='null' then null else trim(gestiones.ide_gest_sector_relac) end AS cod_rec_gestion_sector_relac ,
	--gestiones.ide_gest_sector_relac cod_rec_gestion_sector_relac ,
	trim(gestiones.ide_gest_nro_relac) cod_rec_gestion_nro_relac ,
	trim(gestiones.fec_gestion_alta) dt_rec_gestion_alta ,
	case when trim(gestiones.cod_bandeja_actual)='null' then null else trim(gestiones.cod_bandeja_actual) end AS ds_rec_bandeja_actual ,
	trim(gestiones.fec_bandeja_actual) ts_rec_bandeja_actual ,
	case when trim(gestiones.cod_sector_actual)='null' then null else trim(gestiones.cod_sector_actual) end AS cod_rec_sector_actual ,
	trim(gestiones.inic_user_alta) cod_rec_inic_user_alta ,
	case when trim(gestiones.indi_mail)='null' then null else trim(gestiones.indi_mail) end AS cod_rec_mail ,
	--gestiones.indi_mail cod_rec_mail ,
	trim(gestiones.indi_prioridad) cod_rec_prioridad ,
	trim(gestiones.fec_max_resol) dt_rec_max_resol ,
	trim(gestiones.cod_conforme) cod_rec_conforme ,
	case when trim(gestiones.cod_user_actual)='null' then null else trim(gestiones.cod_user_actual) end AS cod_rec_user_actual ,
	trim(gestiones.cod_grupo_empresa) cog_rec_grupo_empresa ,
	trim(gestiones.cod_tipo_favorabilidad) cod_rec_tipo_favorabilidad ,
	from_unixtime(unix_timestamp(TRIM(gestiones.fec_aviso_venc) , 'dd/MM/yyyy HH:mm:ss'), 'yyyy-MM-dd') dt_rec_aviso_vencimiento,
	circuito.cod_rec_circuito_secundario ,
	circuito.dt_rec_vig_desde_circuito ,
	circuito.dt_rec_vig_hasta_circuito ,
	circuito.ds_rec_circuito ,
	circuito.ds_rec_detalle_circuito ,
	circuito.cod_rec_tipo_gestion ,
	tipo_gestion.ds_rec_tipo_gestion ,
	tipo_gestion.ds_rec_detalle_tipo_gestion ,
	circuito.cod_rec_producto ,
	producto.ds_rec_producto ,
	producto.cod_rec_estado_producto ,
	circuito.cod_rec_subproducto ,
	subproducto.ds_rec_subproducto ,
	subproducto.cod_rec_estado_subproducto ,
	circuito.cod_rec_concepto ,
	concepto.ds_rec_concepto ,
	concepto.ds_rec_detalle_concepto ,
	concepto.cod_rec_estado_concepto ,
	gc_conceptos_sac.ds_rec_concepto_sac ,
	gc_conceptos_sac.cod_rec_tipo_reclamo_sac ,
	gc_conceptos_sac.cod_rec_estado_concepto_sac ,
	gc_conceptos_bcra.ds_rec_concepto_bcra ,
	gc_conceptos_bcra.cod_rec_tipo_reclamo_bcra ,
	gc_conceptos_bcra.cod_rec_estado_concepto_bcra ,
	gc_conceptos_espana.ds_rec_concepto_espana ,
	gc_conceptos_espana.cod_rec_tipo_reclamo_espana ,
	gc_conceptos_espana.cod_rec_estado_concepto_espana ,
	circuito.cod_rec_subconcepto_circuito ,
	circuito.int_rec_tmp_asign_especial_circuito ,
	circuito.int_rec_tmp_pedido_info_circuito ,
	circuito.int_rec_tmp_circuito ,
	circuito.flag_rec_gestion_pendiente_circuito ,
	circuito.flag_rec_mail_demora_circuito ,
	circuito.cod_rec_cierre_automatico_circuito ,
	circuito.flag_rec_autoriza_item_circuito ,
	circuito.cod_rec_recibo_circuito ,
	circuito.cod_rec_estado_circuito ,
	circuito.cod_rec_usuario_alta_circuito ,
	circuito.dt_rec_alta_circuito ,
	circuito.cod_rec_usuario_modif_circuito ,
	circuito.dt_rec_modif_circuito ,
	circuito.cod_rec_modif_datos_circuito ,
	circuito.flag_rec_recep_resp_circuito ,
	circuito.flag_rec_sucursales_circuito ,
	circuito.flag_rec_dejar_pend_circuito ,
	circuito.cod_rec_tipo_resolucion_circuito ,
	circuito.cod_rec_und_tiempo_circuito ,
	circuito.cod_rec_tipo_vist_gest_circuito ,
	circuito.cod_rec_suma_tmp_autz_circuito ,
	circuito.cod_rec_conforme_circuito ,
	circuito.flag_rec_jerarquia_autz_circuito ,
	circuito.flag_rec_secuencia_autz_circuito ,
	circuito.flag_rec_asig_paralelo_circuito ,
	circuito.flag_rec_autoriz_devol_circuito ,
	circuito.flag_rec_modf_fec_circuito ,
	circuito.cod_rec_circuito_predecesor_circuito ,
	circuito.flag_rec_crit_circuito ,
	circuito.flag_rec_info_multivaluada_circuito ,
	circuito.flag_rec_enviar_mail_recep_circuito ,
	circuito.flag_rec_enviar_sms_recep_circuito ,
	circuito.flag_rec_enviar_mail_resp_circuito ,
	circuito.flag_rec_enviar_sms_resp_circuito ,
	circuito.flag_rec_enviar_mail_demora_circuito ,
	circuito.cod_rec_modelo_msj_circuito ,
	circuito.cod_rec_clasif_select_circuito ,
	circuito.cod_rec_parrafo_cuerpo_mail_circuito ,
	circuito.flag_rec_uso_monto_circuito ,
	circuito.flag_rec_indi_enviar_carta_resp_circuito ,
	circuito.cod_rec_comprobante_cliente_circuito ,
	circuito.flag_rec_enviar_mail_prov_circuito ,
	circuito.cod_rec_reapertura_circuito ,
	circuito.cod_rec_long_comentario_recep_circuito ,
	circuito.cod_rec_long_comentario_cliente_circuito ,
	circuito.int_rec_aviso_ven_gest_circuito ,
	circuito.flag_rec_enviar_mail_no_autz_circuito ,
	circuito.cod_rec_entidad_afect_circuito ,
	sectores.ds_rec_sector ,
	sectores.cod_rec_grupo_sector ,
	sectores.cod_rec_actral_sector ,
	sectores.cod_rec_pcia_sector ,
	sectores.ds_rec_mail_sector ,
	sectores.cod_rec_estado_sector ,
	sectores.flag_rec_enviar_mail_sector ,
	sectores.flag_rec_info_asig_esp_sector ,
	sectores.cod_rec_pais_sector ,
	sectores.flag_rec_resol_info_adj_sector ,
	sectores.cod_rec_ctro_costos_sector ,
	sectores.flag_rec_mail_bandeja_sector ,
	sectores.flag_rec_uso_carta_sector ,
	sectores.cod_rec_sector_owner_sector ,
	sectores.cod_rec_grupo_empresa_sector ,
	sectores.flag_rec_admin_sector ,
	sectores.fc_rec_gest_x_pag_bandeja_sector ,
	sectores.cod_suc_sucursal_sector ,
	sectores.cod_rec_zona_sector ,
	sectores.cod_rec_tipo_sector ,
	sectores.ds_rec_class_pdf_sector ,
	sectores.fc_rec_cant_gestiones_ant_sector ,
	sectores.cod_rec_sector_gen ,
	sectores.flag_rec_compromiso_gold_sector ,
	sectores.flag_rec_enviar_mail_resp_sector ,
	sectores.flag_rec_enviar_sms_resp_sector ,
	sectores_actual.ds_rec_sector ds_rec_sector_actual ,
	sectores_actual.cod_rec_grupo_sector cod_rec_grupo_sector_actual ,
	sectores_actual.cod_rec_actral_sector cod_rec_actral_sector_actual ,
	sectores_actual.cod_rec_pcia_sector cod_rec_pcia_sector_actual ,
	sectores_actual.ds_rec_mail_sector ds_rec_mail_sector_actual ,
	sectores_actual.cod_rec_estado_sector cod_rec_estado_sector_actual ,
	sectores_actual.flag_rec_enviar_mail_sector flag_rec_enviar_mail_sector_actual ,
	sectores_actual.flag_rec_info_asig_esp_sector flag_rec_info_asig_esp_sector_actual ,
	sectores_actual.cod_rec_pais_sector cod_rec_pais_sector_actual ,
	sectores_actual.flag_rec_resol_info_adj_sector flag_rec_resol_info_adj_sector_actual ,
	sectores_actual.cod_rec_ctro_costos_sector cod_rec_ctro_costos_sector_actual ,
	sectores_actual.flag_rec_mail_bandeja_sector flag_rec_mail_bandeja_sector_actual ,
	sectores_actual.flag_rec_uso_carta_sector flag_rec_uso_carta_sector_actual ,
	sectores_actual.cod_rec_sector_owner_sector cod_rec_sector_owner_sector_actual ,
	sectores_actual.cod_rec_grupo_empresa_sector cod_rec_grupo_empresa_sector_actual ,
	sectores_actual.flag_rec_admin_sector flag_rec_admin_sector_actual ,
	sectores_actual.fc_rec_gest_x_pag_bandeja_sector fc_rec_gest_x_pag_bandeja_sector_actual ,
	sectores_actual.cod_suc_sucursal_sector cod_suc_sucursal_sector_actual ,
	sectores_actual.cod_rec_zona_sector cod_rec_zona_sector_actual ,
	sectores_actual.cod_rec_tipo_sector cod_rec_tipo_sector_actual ,
	sectores_actual.ds_rec_class_pdf_sector ds_rec_class_pdf_sector_actual ,
	sectores_actual.fc_rec_cant_gestiones_ant_sector fc_rec_cant_gestiones_ant_sector_actual ,
	sectores_actual.cod_rec_sector_gen cod_rec_sector_gen_actual ,
	sectores_actual.flag_rec_compromiso_gold_sector flag_rec_compromiso_gold_sector_actual ,
	sectores_actual.flag_rec_enviar_mail_resp_sector flag_rec_enviar_mail_resp_sector_actual ,
	sectores_actual.flag_rec_enviar_sms_resp_sector flag_rec_enviar_sms_resp_sector_actual ,
	trim(gc_gestiones.nro_cta) cod_rec_nro_cta ,
	case when trim(gc_gestiones.nro_firm_cta)='null' then null else trim(gc_gestiones.nro_firm_cta) end AS cod_rec_nro_firm_cta ,
	--gc_gestiones.nro_firm_cta cod_rec_nro_firm_cta ,
	case when trim(gc_gestiones.cod_mone_cta)='null' then null else trim(gc_gestiones.cod_mone_cta) end AS cod_rec_mone_cta ,
	--gc_gestiones.cod_mone_cta cod_rec_mone_cta ,
	case when trim(gc_gestiones.cod_prod_cta)='null' then null else trim(gc_gestiones.cod_prod_cta) end AS cod_rec_prod_cta ,
	--gc_gestiones.cod_prod_cta cod_rec_prod_cta ,
	case when trim(gc_gestiones.cod_subpro_cta)='null' then null else trim(gc_gestiones.cod_subpro_cta) end AS cod_rec_subpro_cta ,
	trim(gc_gestiones.nro_tarjeta) cod_rec_nro_tarjeta ,
	case when trim(gc_gestiones.link_resumen)='null' then null else trim(gc_gestiones.link_resumen) end AS ds_rec_link_resumen ,
	--gc_gestiones.link_resumen ds_rec_link_resumen ,
	case when trim(gc_gestiones.dire_mail_contc)='null' then null else trim(gc_gestiones.dire_mail_contc) end AS ds_rec_dire_mail_contc ,
	--gc_gestiones.dire_mail_contc ds_rec_dire_mail_contc ,
	case when trim(gc_gestiones.dire_mail2_contc)='null' then null else trim(gc_gestiones.dire_mail2_contc) end AS ds_rec_dire_mail2_contc ,
	--gc_gestiones.dire_mail2_contc ds_rec_dire_mail2_contc ,
	trim(gc_gestiones.cartera) int_rec_cartera ,
	case when trim(gc_gestiones.aplic_cta_gold)='null' then null else trim(gc_gestiones.aplic_cta_gold) end AS cod_rec_aplic_cta_gold ,
	case when trim(gc_gestiones.cod_suc_cta_gold)='null' then null else trim(gc_gestiones.cod_suc_cta_gold) end AS cod_suc_sucursal_cta_gold ,
	--gc_gestiones.cod_suc_cta_gold cod_suc_sucursal_cta_gold ,
	trim(gc_gestiones.nro_cta_gold) cod_rec_cta_gold ,
	trim(gc_gestiones.nro_firm_cta_gold) cod_rec_nro_firm_cta_gold ,
	case when trim(gc_gestiones.cod_mone_cta_gold)='null' then null else trim(gc_gestiones.cod_mone_cta_gold) end AS cod_rec_mone_cta_gold ,
	--gc_gestiones.cod_mone_cta_gold cod_rec_mone_cta_gold ,
	case when trim(gc_gestiones.cod_prod_cta_gold)='null' then null else trim(gc_gestiones.cod_prod_cta_gold) end AS cod_rec_prod_cta_gold ,
	--gc_gestiones.cod_prod_cta_gold cod_rec_prod_cta_gold ,
	case when trim(gc_gestiones.cod_subpro_cta_gold)='null' then null else trim(gc_gestiones.cod_subpro_cta_gold) end AS cod_rec_subpro_cta_gold ,
	--gc_gestiones.cod_subpro_cta_gold cod_rec_subpro_cta_gold ,
	trim(gc_gestiones.cod_ramo_cta) cod_rec_ramo_cta ,
	trim(gc_gestiones.nro_certificado_cta) cod_rec_nro_certificado_cta ,
	trim(gc_gestiones.rentabilidad_promedio) fc_rec_rentabilidad_promedio ,
	case when trim(gc_gestiones.color_semaforo)='null' then null else trim(gc_gestiones.color_semaforo) end AS cod_rec_color_semaforo ,
	--gc_gestiones.color_semaforo cod_rec_color_semaforo ,
	case when trim(gc_gestiones.color_semaf_riesgo)='null' then null else trim(gc_gestiones.color_semaf_riesgo) end AS cod_rec_color_semaf_riesgo ,
	--gc_gestiones.color_semaf_riesgo cod_rec_color_semaf_riesgo ,
	trim(gc_gestiones.indi_envios_mya) cod_rec_envios_mya ,
	case when trim(gc_gestiones.dire_mail_opcional)='null' then null else trim(gc_gestiones.dire_mail_opcional) end AS ds_rec_dire_mail_opcional ,
	--gc_gestiones.dire_mail_opcional ds_rec_dire_mail_opcional ,
	trim(gc_gestiones.nro_paquete) cod_rec_nro_paquete ,
	trim(gc_gestiones.cod_paquete) cod_rec_paquete ,
	case when gc_gestiones.indi_plan_sueldo='S' then 1 else 0 end flag_rec_plan_sueldo ,
	case when trim(gc_gestiones.nro_convenio_paquete)='null' then null else trim(gc_gestiones.nro_convenio_paquete) end AS cod_rec_nro_convenio_paquete ,
	case when trim(gc_gestiones.emp_celular_contc)='null' then null else trim(gc_gestiones.emp_celular_contc) end AS ds_rec_emp_celular_contc ,
	--gc_gestiones.emp_celular_contc ds_rec_emp_celular_contc ,
	case when trim(gc_gestiones.tpo_msj_asoc_rta)='null' then null else trim(gc_gestiones.tpo_msj_asoc_rta) end AS cod_rec_tipo_msj_asoc_rta ,
	trim(gc_gestiones.monto_deuda_ref) fc_rec_monto_deuda_ref ,
	case when gc_gestiones.no_enviar_notificaciones='S' then 1 else 0 end flag_rec_no_enviar_notificaciones ,
	case when trim(gc_gestiones.id_ejecutivo)='null' then null else trim(gc_gestiones.id_ejecutivo) end AS cod_rec_id_ejecutivo ,
	--gc_gestiones.id_ejecutivo cod_rec_id_ejecutivo ,
	gestiones.partition_date partition_date
FROM
	bi_corp_staging.rio56_gestiones gestiones
LEFT JOIN bi_corp_common.dim_rec_circuito circuito ON
	gestiones.cod_entidad = circuito.cod_rec_entidad
	AND gestiones.ide_circuito = circuito.cod_rec_circuito
LEFT JOIN bi_corp_common.dim_rec_tipo_gestion tipo_gestion ON
	circuito.cod_rec_entidad = tipo_gestion.cod_rec_entidad
	AND circuito.cod_rec_tipo_gestion = tipo_gestion.cod_rec_tipo_gestion
LEFT JOIN bi_corp_common.dim_rec_producto producto ON
	circuito.cod_rec_entidad = producto.cod_rec_entidad
	AND circuito.cod_rec_producto = producto.cod_rec_producto
LEFT JOIN bi_corp_common.dim_rec_subproducto subproducto ON
	circuito.cod_rec_entidad = subproducto.cod_rec_entidad
	AND circuito.cod_rec_subproducto = subproducto.cod_rec_subproducto
LEFT JOIN bi_corp_common.dim_rec_concepto concepto ON
	circuito.cod_rec_entidad = concepto.cod_rec_entidad
	AND circuito.cod_rec_concepto = concepto.cod_rec_concepto
LEFT JOIN bi_corp_common.dim_rec_concepto_sac_circ gc_cpto_sac_circ ON
	circuito.cod_rec_entidad = gc_cpto_sac_circ.cod_rec_entidad
	AND circuito.cod_rec_circuito = gc_cpto_sac_circ.cod_rec_circuito
	AND gc_cpto_sac_circ.cod_rec_estado_concepto_sac = 'A'
LEFT JOIN bi_corp_common.dim_rec_concepto_sac gc_conceptos_sac ON
	gc_cpto_sac_circ.cod_rec_entidad = gc_conceptos_sac.cod_rec_entidad
	AND gc_cpto_sac_circ.cod_rec_concepto_sac = gc_conceptos_sac.cod_rec_concepto_sac
LEFT JOIN bi_corp_common.dim_rec_concepto_bcra_circ gc_cpto_bcra_circ ON
	circuito.cod_rec_entidad = gc_cpto_bcra_circ.cod_rec_entidad
	AND circuito.cod_rec_circuito = gc_cpto_bcra_circ.cod_rec_circuito
	AND gc_cpto_bcra_circ.cod_rec_estado_concepto_bcra = 'a'
LEFT JOIN bi_corp_common.dim_rec_concepto_bcra gc_conceptos_bcra ON
	gc_cpto_bcra_circ.cod_rec_entidad = gc_conceptos_bcra.cod_rec_entidad
	AND gc_cpto_bcra_circ.cod_rec_concepto_bcra = gc_conceptos_bcra.cod_rec_concepto_bcra
LEFT JOIN bi_corp_common.dim_rec_concepto_espana_circ gc_cpto_espana_circ ON
	circuito.cod_rec_entidad = gc_cpto_espana_circ.cod_rec_entidad
	AND circuito.cod_rec_circuito = gc_cpto_espana_circ.cod_rec_circuito
	AND gc_cpto_espana_circ.cod_rec_estado_concepto_espana = 'a'
LEFT JOIN bi_corp_common.dim_rec_concepto_espana gc_conceptos_espana ON
	gc_cpto_espana_circ.cod_rec_entidad = gc_conceptos_espana.cod_rec_entidad
	AND gc_cpto_espana_circ.cod_rec_concepto_espana = gc_conceptos_espana.cod_rec_concepto_espana
LEFT JOIN bi_corp_common.dim_rec_sector sectores ON
	gestiones.ide_gestion_sector = sectores.cod_rec_sector
	AND gestiones.cod_entidad = sectores.cod_rec_entidad
LEFT JOIN bi_corp_common.dim_rec_sector sectores_actual ON
	gestiones.cod_sector_actual = sectores_actual.cod_rec_sector
	AND gestiones.cod_entidad = sectores_actual.cod_rec_entidad
LEFT JOIN bi_corp_staging.rio56_gc_gestiones gc_gestiones ON
	gestiones.cod_entidad = gc_gestiones.cod_entidad
	AND gestiones.ide_gestion_nro = gc_gestiones.ide_gestion_nro
	AND gc_gestiones.partition_date = gestiones.partition_date	
LEFT JOIN bi_corp_common.dim_rec_gest_individuos gc_individuo_gest ON
	gestiones.cod_entidad = gc_individuo_gest.cod_rec_entidad 
	AND gestiones.ide_gestion_nro = gc_individuo_gest.cod_rec_gestion_nro 
LEFT JOIN bi_corp_common.dim_rec_gest_empresas gc_empresa_gest ON
	gestiones.cod_entidad = gc_empresa_gest.cod_rec_entidad
	AND gestiones.ide_gestion_nro = gc_empresa_gest.cod_rec_gestion_nro

WHERE gestiones.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_SGC-Daily_scheduled') }}'
