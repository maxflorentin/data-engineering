set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT OVERWRITE TABLE bi_corp_common.stk_rec_gestiones_csm
PARTITION(partition_date)
SELECT
	TRIM(gestiones.id_gestion) AS cod_rec_gestion,
	clientes.cod_per_nup,
	clientes.ds_rec_numdoc,
	clientes.ds_rec_nombre_razonsocial,
	clientes.ds_rec_apellido_nom_fantasia,
	TRIM(gestiones.id_casuistica) AS cod_rec_casuistica,
	case when trim(gestiones.aplicacion_contrato)='null' then null else trim(gestiones.aplicacion_contrato) end AS cod_rec_aplicacion_contrato,
	TRIM(gestiones.producto_contrato) AS cod_rec_producto_contrato,
	TRIM(gestiones.subproducto_contrato) AS cod_rec_subproducto_contrato,
	TRIM(gestiones.sucursal_contrato) AS cod_suc_sucursal_contrato,
	TRIM(gestiones.nro_contrato) AS cod_rec_nro_contrato,
	case when trim(gestiones.divisa_contrato)='null' then null else trim(gestiones.divisa_contrato) end AS cod_rec_divisa_contrato,
	TRIM(gestiones.monto_reclamado) AS fc_rec_monto_reclamado,
	gestiones.monto_devuelto AS fc_rec_monto_devuelto,
	case when trim(gestiones.cod_motivo)='null'then null else trim(gestiones.cod_motivo) end AS cod_rec_motivo,
	--case when trim(gestiones.desc_motivo)='null' then null else trim(gestiones.desc_motivo) end AS ds_rec_motivo,
	casuistica_motivos.ds_rec_motivo,
	TRIM(gestiones.comentario) AS ds_rec_comentario,
	TRIM(gestiones.estado_actual) AS cod_rec_estado_actual,
	TRIM(gestiones.id_usuario_alta) AS cod_rec_usuario_alta,
	TRIM(gestiones.fecha_alta) AS ts_rec_alta,
	case when trim(gestiones.id_usuario_modif)='null' then null else trim(gestiones.id_usuario_modif) end AS cod_rec_usuario_modif,
	TRIM(gestiones.fecha_modif) AS ts_rec_modif,
	TRIM(gestiones.actor_actual) AS cod_rec_actor_actual,
	case when trim(gestiones.producto_paquete)='null' then null else trim(gestiones.producto_paquete) end AS cod_rec_producto_paquete,
	case when trim(gestiones.subproducto_paquete)='null' then null else trim(gestiones.subproducto_paquete) end AS cod_rec_subproducto_paquete,
	TRIM(gestiones.id_mail_resolucion) AS cod_rec_mail_resolucion,
	TRIM(gestiones.cod_grupo_motdes) AS cod_rec_grupo_motdes,
	case when trim(gestiones.digito_verificador)='null' then null else trim(gestiones.digito_verificador) end AS cod_rec_digito_verificador,
	case when trim(gestiones.id_usuario_actual)='null' then null else trim(gestiones.id_usuario_actual) end AS cod_rec_usuario_actual,
	CASE WHEN TRIM(gestiones.tiene_sorpresa)='S' then 1 else 0 end AS flag_rec_tiene_sorpresa,
	TRIM(gestiones.monto_reclamado_usd) AS fc_rec_monto_reclamado_usd,
	gestiones.monto_devuelto_usd AS fc_rec_monto_devuelto_usd,
	to_date(gestiones.fecha_resolucion) dt_rec_resolucion,
	TRIM(gestiones.fecha_estado_actual) AS ts_rec_estado_actual,
	to_date(gestiones.fecha_estado_max)  dt_rec_estado_max,
	case when trim(gestiones.cod_respuesta)='null' then null else trim(gestiones.cod_respuesta) end AS cod_rec_respuesta,
	TRIM(gestiones.cod_resultado) AS cod_rec_resultado,
	case when trim(gestiones.comentario_resolutor)='null' then null else trim(gestiones.comentario_resolutor) end AS ds_rec_comentario_resolutor,
	TRIM(gestiones.fecha_baja)  AS ts_rec_baja,
	TRIM(gestiones.id_canal) AS cod_rec_canal,
	TRIM(gestiones.ind_favorabilidad_mot_des) AS cod_rec_favorabilidad_mot_des,
	TRIM(gestiones.ind_favorabilidad_resol) AS cod_rec_favorabilidad_resol,
	CASE WHEN TRIM(gestiones.informa_bcra)='S' then 1 else 0 end  AS flag_rec_informa_bcra,
	TRIM(gestiones.nro_gestion) AS cod_rec_nro_gestion,
	TRIM(gestiones.cod_resolucion) AS cod_rec_resolucion,
	case when trim(gestiones.desc_resolucion)='null' then null else trim(gestiones.desc_resolucion) end AS ds_rec_resolucion,
	TRIM(gestiones.nro_ticket) AS cod_rec_nro_ticket,
	case when trim(gestiones.prefijo_ticket)='null' then null else trim(gestiones.prefijo_ticket) end AS ds_rec_prefijo_ticket,
	case when trim(gestiones.nro_reclamo_bco)='null' then null else trim(gestiones.nro_reclamo_bco) end AS cod_rec_nro_reclamo_bco,
	case when trim(gestiones.id_impuesto)='null' then null else trim(gestiones.id_impuesto) end AS cod_rec_impuesto,
	CASE WHEN TRIM(gestiones.ind_ajuste)='S' then 1 else 0 end   AS flag_rec_ajuste,
	CASE WHEN TRIM(gestiones.ind_anticipo)='S' then 1 else 0 end   AS flag_rec_anticipo,
	case when trim(gestiones.origen_reclamo)='null' then null else trim(gestiones.origen_reclamo) END  AS cod_rec_origen_reclamo,
	case when trim(gestiones.zona_consumo)='null' then null else trim(gestiones.zona_consumo) end AS cod_rec_zona_consumo,
	TRIM(gestiones.etapa_resolucion) AS cod_rec_etapa_resolucion,
	case when trim(gestiones.cod_segmento)='null' then null else trim(gestiones.cod_segmento) end AS cod_rec_segmento,
	case when trim(gestiones.cod_subsegmento)='null' then null else trim(gestiones.cod_subsegmento) end AS cod_rec_subsegmento,
	CASE WHEN TRIM(gestiones.v_ind_ajuste_provisorio)='S' then 1 else 0 end AS flag_rec_ajuste_provisorio,
	CASE WHEN TRIM(gestiones.ejecutivo_on_line)='SI' then 1 else 0 end AS flag_rec_ejecutivo_online,
	TRIM(gestiones.monto_iva) AS fc_rec_monto_iva,
	case when trim(gestiones.cod_suc_alta)='null' then null else trim(gestiones.cod_suc_alta) END AS cod_suc_sucursal_alta,
	case when trim(gestiones.cod_ofic_alta)='null' then null else trim(gestiones.cod_ofic_alta) END AS cod_rec_oficial_alta,
	case when trim(gestiones.id_actor_alta)='null' then null else trim(gestiones.id_actor_alta) end AS cod_rec_actor_alta,
	case when trim(gestiones.id_requerimiento_premiacion)='null' then null else trim(gestiones.id_requerimiento_premiacion) end AS cod_rec_requerimiento_premiacion,
	case when trim(gestiones.cuenta_paquete)='null' then null else trim(gestiones.cuenta_paquete) end AS cod_rec_cuenta_paquete,
	case when trim(gestiones.nro_paquete)='null' then null else trim(gestiones.nro_paquete) end AS cod_rec_nro_paquete,
	case when trim(gestiones.sucursal_paquete)='null' then null else trim(gestiones.sucursal_paquete) end AS cod_suc_sucursal_paquete,
    case when trim(gestiones.tipo_persona)='null' then null else trim(gestiones.tipo_persona) end AS cod_rec_tipo_persona,
	CASE WHEN TRIM(gestiones.cliente_vip)='SI' then 1 else 0 end AS  flag_rec_cliente_vip,
	TRIM(gestiones.cant_cheque_reclamado) AS fc_rec_cantidad_cheque_rec,
	TRIM(gestiones.fecha_hora_operacion)AS ts_rec_operacion,
	case when trim(gestiones.cod_suc_resol)='null' then null else trim(gestiones.cod_suc_resol) end AS cod_suc_sucursal_resol,
	case when trim(gestiones.cod_zona_resol)='null' then null else trim(gestiones.cod_zona_resol) end AS cod_rec_zona_resol,
	case when trim(gestiones.cod_bco_emisor)='null' then null else trim(gestiones.cod_bco_emisor) end  AS cod_rec_banco_emisor,
	actor_actual.cod_rec_tipo_actor AS cod_rec_tipo_actor_actual,
	actor_actual.ds_rec_actor AS ds_rec_actor_actual,
	actor_alta.cod_rec_tipo_actor AS cod_rec_tipo_actor_alta,
	actor_alta.ds_rec_actor AS ds_rec_actor_alta,
	casuisticas.ds_rec_casuistica,
	casuisticas.cod_rec_tipo_gestion,
	casuisticas.cod_rec_tipologia,
	casuisticas.fc_rec_cant_meses_primer_reclamo,
	casuisticas.fc_rec_cant_dias_resolucion,
	casuisticas.cod_rec_tipo_producto,
	casuisticas.cod_rec_sector_soporte,
	tipos_gestion.ds_rec_tipo_gestion,
	tipos_gestion.flag_rec_informa_bcra,
	tipos_gestion.ds_rec_prefijo_ticket AS ds_rec_prefijo_ticket_tipo_gestion,
	tipos_gestion.cod_rec_tipo_gestion AS cod_rec_tipo_gestion_sgc,
	canales.ds_rec_canal,
	usuarios_alta.ds_rec_usuario_nombre AS ds_rec_usuario_alta_nombre,
	usuarios_alta.ds_rec_usuario_apellido AS ds_rec_usuario_alta_apellido,
	usuarios_alta.cod_rec_oficina as cod_rec_oficina_usu_alta,
	usuarios_alta.cod_suc_sucursal as cod_suc_sucursal_usu_alta,
	centro_costos_alta.ds_rec_centro_costo AS ds_rec_centro_costo_usu_alta,
	centro_costos_alta.cod_rec_zona as cod_rec_zona_alta,
	usuarios_modif.ds_rec_usuario_nombre AS ds_rec_usuario_modif_nombre,
	usuarios_modif.ds_rec_usuario_apellido AS ds_rec_usuario_modif_apellido,
	usuarios_modif.cod_rec_oficina as cod_rec_oficina_usu_modif,
	usuarios_modif.cod_suc_sucursal as cod_suc_sucursal_usu_modif,
	centro_costos_modif.ds_rec_centro_costo AS ds_rec_centro_costo_usu_modif,
	centro_costos_modif.cod_rec_zona as cod_rec_zona_modif,
	usuarios_actual.ds_rec_usuario_nombre AS ds_rec_usuario_actual_nombre,
	usuarios_actual.ds_rec_usuario_apellido AS ds_rec_usuario_actual_apellido,
	usuarios_actual.cod_rec_oficina as cod_rec_oficina_usu_actual,
	usuarios_actual.cod_suc_sucursal as cod_suc_sucursal_usu_actual,
	centro_costos_actual.ds_rec_centro_costo AS ds_rec_centro_costo_usu_actual,
	centro_costos_actual.cod_rec_zona as cod_rec_zona_actual,
	motdes_arbol_resultados.ds_rec_resultado AS ds_rec_resultado_arbol_resul,
	motdes_arbol_resultados.ds_rec_mensaje AS ds_rec_texto_msj_arbol_resul,
	motdes_arbol_resultados.cod_rec_favorabilidad AS cod_rec_favorabilidad_arbol_resul,
	--motdes_acciones.cod_rec_accion as cod_rec_motdes_accion,
	--resol_acciones.cod_rec_accion as cod_rec_resol_accion,
	gestiones.partition_date
FROM
	bi_corp_staging.rio187_gestiones gestiones
LEFT JOIN bi_corp_common.dim_rec_actor actor_actual ON
	TRIM(gestiones.actor_actual) = actor_actual.cod_rec_actor
LEFT JOIN bi_corp_common.dim_rec_actor actor_alta ON
	TRIM(gestiones.id_actor_alta) = actor_alta.cod_rec_actor
LEFT JOIN bi_corp_common.dim_rec_casuistica casuisticas ON
	cast(gestiones.id_casuistica as int) = casuisticas.cod_rec_casuistica
LEFT JOIN bi_corp_common.dim_rec_casuistica_motivos casuistica_motivos ON
	TRIM(gestiones.id_casuistica) = casuistica_motivos.cod_rec_casuistica
	and TRIM(gestiones.cod_motivo) = casuistica_motivos.cod_rec_motivo
LEFT JOIN bi_corp_common.dim_rec_tipo_gestion tipos_gestion ON
	casuisticas.cod_rec_tipo_gestion = cast(tipos_gestion.cod_rec_tipo_gestion_cosmos as int)
LEFT JOIN bi_corp_common.dim_rec_canal canales ON
	cast(gestiones.id_canal as int) = canales.cod_rec_canal_cosmos
LEFT JOIN bi_corp_common.dim_rec_usuario usuarios_alta ON
	TRIM(gestiones.id_usuario_alta) = usuarios_alta.cod_rec_usuario
LEFT JOIN bi_corp_common.dim_rec_usuario usuarios_modif ON
	TRIM(gestiones.id_usuario_modif) = usuarios_modif.cod_rec_usuario
LEFT JOIN bi_corp_common.dim_rec_usuario usuarios_actual ON
	TRIM(gestiones.id_usuario_actual) = usuarios_actual.cod_rec_usuario
LEFT JOIN bi_corp_common.dim_rec_centro_costo centro_costos_alta ON
	usuarios_alta.cod_rec_oficina = centro_costos_alta.cod_rec_oficina
	AND usuarios_alta.cod_suc_sucursal = centro_costos_alta.cod_suc_sucursal
LEFT JOIN bi_corp_common.dim_rec_centro_costo centro_costos_modif ON
	usuarios_modif.cod_rec_oficina = centro_costos_modif.cod_rec_oficina
	AND usuarios_modif.cod_suc_sucursal = centro_costos_modif.cod_suc_sucursal
LEFT JOIN bi_corp_common.dim_rec_centro_costo centro_costos_actual ON
	usuarios_actual.cod_rec_oficina = centro_costos_actual.cod_rec_oficina
	AND usuarios_actual.cod_suc_sucursal = centro_costos_actual.cod_suc_sucursal
LEFT JOIN bi_corp_common.dim_rec_motdes_arbol_resultado motdes_arbol_resultados ON
	cast(gestiones.id_casuistica as int) = motdes_arbol_resultados.cod_rec_casuistica
	AND cast(gestiones.cod_resultado as int) = motdes_arbol_resultados.cod_rec_resultado
LEFT JOIN bi_corp_common.dim_rec_clientes clientes ON
    cast(gestiones.id_cliente AS int) = clientes.cod_rec_cliente
--LEFT JOIN bi_corp_common.stk_rec_gest_motdes_acciones motdes_acciones ON
--    TRIM(gestiones.id_gestion) = motdes_acciones.cod_rec_gestion
--    and motdes_acciones.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cosmos') }}'
--LEFT JOIN bi_corp_common.stk_rec_gest_resol_acciones resol_acciones ON
--    TRIM(gestiones.id_gestion) = resol_acciones.cod_rec_gestion
--    and resol_acciones.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cosmos') }}'
WHERE gestiones.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cosmos') }}';