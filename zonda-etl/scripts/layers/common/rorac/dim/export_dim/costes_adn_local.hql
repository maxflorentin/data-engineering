"
set mapred.job.queue.name=root.dataeng ;

SELECT DISTINCT 
	'ARG' cod_ren_unidad 
    , RE.cod_area_negocio 
FROM bi_corp_staging.rio157_ms0_ft_dwh_blce_result RE
WHERE RE.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='RORAC_EXPORT_Tables') }}'
"		
