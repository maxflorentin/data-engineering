"
set mapred.job.queue.name=root.dataeng ;

SELECT DISTINCT RE.cod_centro_cont 
	, UPPER(CEN.des_hijo) ds_ren_centro 
FROM (
SELECT RE.fec_data , RE.cod_centro_cont FROM bi_corp_staging.rio157_ms0_ft_dwh_blce_result RE
WHERE RE.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='partition_date', dag_id='RORAC_EXPORT_Tables') }}'
)RE
JOIN bi_corp_staging.rio157_ms0_dm_je_dwh_centros_ctr CEN ON 
	CEN.partition_date = '2020-08-14'
	AND RE.cod_centro_cont = CEN.cod_hijo 
"		
