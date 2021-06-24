set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE bi_corp_common.stk_cys_discrepanciasasdf
PARTITION(partition_date) 
SELECT  CAST(nro_ejecucion AS INT) nro_ejecucion 
	, CAST(periodo AS INT) periodo 
	, CAST(nup AS BIGINT) nup 
	, CAST(nro_documento AS BIGINT) nro_documento 
	, IF(denominacion = 'null', NULL, TRIM(denominacion)) denominacion 
	, IF(clasif_efd_sin_disc = 'null', NULL, TRIM(clasif_efd_sin_disc)) clasif_efd_sin_disc 
	, IF(clasif_discrepancia = 'null', NULL, TRIM(clasif_discrepancia)) clasif_discrepancia 
	, IF(clasif_efd = 'null', NULL, TRIM(clasif_efd)) clasif_efd 
	, deuda_total 
	, prev_total 
	, prev_total_sin_disc
	, partition_date
FROM bi_corp_staging.rio5_clientes
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_to', dag_id='LOAD_CMN_DSF-Monthly') }}' 																																																																																																																																		
	AND nro_ejecucion = '2' ;