"
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE bi_corp_common.dim_ren_genericresult
PARTITION(partition_date) 

SELECT  idf_cto_ods
	, cod_contenido
	, cod_pais
	, from_unixtime(unix_timestamp(fec_alta_cto , 'yyyyMMdd'), 'yyyy-MM-dd') fec_alta_cto
	, from_unixtime(unix_timestamp(fec_ven, 'yyyyMMdd'), 'yyyy-MM-dd') fec_ven
	, from_unixtime(unix_timestamp(fec_reestruc, 'yyyyMMdd'), 'yyyy-MM-dd') fec_reestruc
	, IF(cod_producto = 'null', null, cod_producto) cod_producto 
	, IF(cod_subprodu = 'null', null, cod_subprodu) cod_subprodu
	, partition_date
FROM bi_corp_staging.rio157_ms0_dt_dwh_generic_result
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_to', dag_id='LOAD_CMN-RORAC_inputs') }}' ;
"