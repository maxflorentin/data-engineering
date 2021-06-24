SET mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE bi_corp_common.stk_rec_gest_resol_acciones
PARTITION(partition_date)
	SELECT
		TRIM(a.id_gestion) cod_rec_gestion ,
		TRIM(a.id_accion) cod_rec_accion ,
		TRIM(a.ind_origen) cod_rec_origen ,
		TRIM(a.ind_ejecucion) cod_rec_ejecucion ,
		a.partition_date
	FROM
		bi_corp_staging.rio187_gest_resol_acciones a
	WHERE a.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Cosmos') }}';