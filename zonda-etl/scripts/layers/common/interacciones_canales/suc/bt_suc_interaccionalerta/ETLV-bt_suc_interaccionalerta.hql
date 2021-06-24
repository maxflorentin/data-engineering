SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode = nonstrict;

INSERT 	overwrite TABLE bi_corp_common.bt_suc_interaccionalerta PARTITION (partition_date)
SELECT TRIM(cd_interaccion_alerta) cod_suc_interaccionalerta
	, TRIM(cd_interaccion) cod_suc_interaccion
	, CAST(cd_alerta AS INT) cod_suc_alerta
	, CAST(cd_resultado AS INT) cod_suc_resultado
	, CAST(cd_gestion AS INT) cod_suc_gestion
	, to_date(dt_gestion) dt_suc_gestion
	, SUBSTRING(dt_gestion, 1, 19) ts_suc_gestion
	, TRIM(clave_unica) ds_suc_claveunica
	, TRIM(json) ds_suc_json
	, TRIM(ds_contacto) ds_suc_contacto
	, CAST(cd_identificacion_resultado AS INT) cod_suc_identificacionresultado
	, partition_date
FROM bi_corp_staging.rio151_tbl_interaccion_alerta 
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='LOAD_CMN_Sucursales') }}' ;