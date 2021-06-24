SET mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT
	OVERWRITE TABLE bi_corp_common.dim_rec_casuistica
SELECT
	TRIM(ori.id_casuistica) cod_rec_casuistica ,
	TRIM(UPPER(ori.desc_casuistica))  ds_rec_casuistica ,
	TRIM(ori.id_tipo_gestion) cod_res_tipo_gestion ,
	TRIM(ori.id_tipologia) cod_res_tipologia ,
	COALESCE(cast(ori.cant_meses_primer_reclamo as int),0) fc_rec_cant_meses_primer_reclamo ,
	COALESCE(cast(ori.cant_dias_resolucion as int),0) fc_rec_cant_dias_resolucion ,
	TRIM(ori.tipo_producto) cod_rec_tipo_producto ,
	TRIM(ori.id_sector_soporte) cod_rec_sector_soporte ,
	case when TRIM(ori.habilitado)='S' then 1 else 0 end  flag_rec_habilitado ,
	case when TRIM(ori.id_usuario_alta)='null' then null else TRIM(ori.id_usuario_alta)  end as cod_rec_usuario_alta,
	to_date(ori.fecha_alta) as dt_rec_casuistica_alta,
	case when TRIM(ori.id_usuario_modif)='null' then null else TRIM(ori.id_usuario_modif)  end as cod_rec_usuario_modif,
	to_date(ori.fecha_modif) as dt_rec_casuistica_modif,
	TRIM(ori.texto_mail_casuistica) ds_rec_texto_mail_casuistica ,
	case when TRIM(ori.ind_interes_comp)='S' then 1 else 0 end  flag_rec_interes_comp ,
	to_date(ori.fecha_baja) as dt_rec_casuistica_baja,
	case when TRIM(ori.ind_solo_activas)='S' then 1 else 0 end  flag_rec_solo_activas ,
	COALESCE(cast(ori.dias_warning_bcra as int),0) fc_rec_dias_warning_bcra ,
	TRIM(ori.id_state) cod_rec_state ,
	case when TRIM(ori.sinonimos)='null' then null else TRIM(ori.sinonimos)  end as ds_rec_sinonimos,
	case when TRIM(ori.nombre_atajos)='null' then null else TRIM(ori.nombre_atajos)  end as ds_rec_nombre_atajos,
	case when TRIM(ori.desc_atajos)='null' then null else TRIM(ori.desc_atajos)  end as ds_rec_desc_atajos,
	case when TRIM(ori.secciones)='null' then null else TRIM(ori.secciones)  end as cod_rec_secciones,
	case when TRIM(ori.monto_desde_sucu)='null' then null else TRIM(ori.monto_desde_sucu)  end as fc_rec_monto_desde_sucu,
	case when TRIM(ori.monto_desde_zona)='null' then null else TRIM(ori.monto_desde_zona)  end as fc_rec_monto_desde_zona,
	case when TRIM(ori.cant_tope_mes)='null' then null else TRIM(ori.cant_tope_mes)  end as fc_rec_cant_tope_mes,
	ori.partition_date partition_date
FROM bi_corp_staging.rio187_casuisticas ori
WHERE ori.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_rio187_casuisticas', dag_id='LOAD_CMN_Cosmos') }}';