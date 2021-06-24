set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT
	OVERWRITE TABLE bi_corp_common.dim_rec_motdes_arbol_resultado
SELECT
	TRIM(arbol.id_casuistica) cod_rec_casuistica ,
	TRIM(arbol.cod_resultado) cod_rec_resultado ,
	TRIM(arbol.desc_resultado) ds_rec_resultado ,
	case when length(trim(arbol.texto_mensaje))=0 or trim(arbol.texto_mensaje)='null'
		 then null
		 else trim(arbol.texto_mensaje)
		 end AS ds_rec_mensaje ,
	TRIM(arbol.ind_favorabilidad) cod_rec_favorabilidad ,
	TRIM(arbol.id_usuario_alta) cod_rec_usuario_alta ,
	to_date(arbol.fecha_alta)dt_rec_fecha_alta ,
	case when TRIM(arbol.id_usuario_modif)='null' then null else TRIM(arbol.id_usuario_modif) end cod_rec_usuario_modif ,
	to_date(arbol.fecha_modif) dt_rec_fecha_modif ,
	to_date(arbol.fecha_baja) dt_rec_fecha_baja ,
	partition_date
FROM
	bi_corp_staging.rio187_motdes_arbol_resultados arbol
WHERE partition_date= '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_rio187_motdes_arbol_resultados', dag_id='LOAD_CMN_Cosmos') }}'