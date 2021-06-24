SET mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT
	OVERWRITE TABLE bi_corp_common.dim_rec_canal
SELECT
	TRIM(ori.id_canal) cod_rec_canal_cosmos ,
	TRIM(ori.cod_canal) cod_rec_canal ,
	UPPER(TRIM(ori.desc_canal)) ds_rec_canal ,
	TRIM(ori.id_usuario_alta) cod_rec_usuario_alta ,
	to_date(ori.fecha_alta) dt_rec_canal_alta ,
	TRIM(ori.id_usuario_modif) cod_rec_usuario_modif ,
	to_date(ori.fecha_modif) dt_rec_canal_modif ,
	to_date(ori.fecha_baja) dt_rec_canal_baja ,
	ori.partition_date dt_rec_upload_date 
FROM
	bi_corp_staging.rio187_canales ori
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_rio187_canales', dag_id='LOAD_CMN_Cosmos') }}'