"
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE bi_corp_common.dim_ren_clienteresult
PARTITION(partition_date) 

SELECT  
	TRIM(idf_pers_ods) ,
	CAST(SUBSTR(idf_pers_ods, 6, 8) AS INT) cod_per_nup	,
	IF(cod_vincula = 'null', NULL, cod_vincula) ,
	cod_tip_persona	,
	CONCAT(IF(nom_nombre = 'null', '', nom_nombre),' ',IF(nom_apellido_1 = 'null', '', nom_apellido_1)) ds_per_nombre_apellido ,
	partition_date
FROM bi_corp_staging.rio157_ms0_dt_dwh_cliente_result
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_to', dag_id='LOAD_CMN-RORAC_inputs') }}' ;
"