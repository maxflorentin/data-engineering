"
set mapred.job.queue.name=root.dataeng ;

SELECT DISTINCT RE.cod_segmento_gest 
	, SE.des_hijo
FROM 
(SELECT DISTINCT RE.cod_segmento_gest 
 FROM bi_corp_staging.rio157_ms0_ft_dwh_blce_result RE
 WHERE RE.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='RORAC_EXPORT_Tables') }}'
) RE
JOIN bi_corp_staging.rio157_ms0_dm_je_dwh_segmentos_ctr SE ON 
	SE.partition_date = '2020-08-12'
	AND RE.cod_segmento_gest = SE.cod_hijo ;
"