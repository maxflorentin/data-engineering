SET mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT
	OVERWRITE TABLE bi_corp_common.dim_rec_actor
SELECT
	TRIM(ori.id_actor) cod_rec_actor ,
	TRIM(ori.tipo_actor) cod_rec_tipo_actor ,
	TRIM(UPPER(ori.desc_actor))  ds_rec_actor ,
	TRIM(ori.id_usuario_alta) cod_rec_usuario_alta ,
	to_date(ori.fecha_alta) as dt_rec_actor_alta,
	case when TRIM(ori.id_usuario_modif)='null' then null else TRIM(ori.id_usuario_modif) end as cod_rec_usuario_modif ,
	to_date(ori.fecha_modif) as dt_rec_actor_modif ,
	ori.partition_date partition_date 
FROM
	bi_corp_staging.rio187_actores ori
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_rio187_actores', dag_id='LOAD_CMN_Cosmos') }}';