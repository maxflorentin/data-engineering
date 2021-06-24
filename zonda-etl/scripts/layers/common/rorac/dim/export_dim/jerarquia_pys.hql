"
set mapred.job.queue.name=root.dataeng ;

SELECT DISTINCT num_orden
	, num_nivel
	, cod_hijo
	, des_hijo
	, IF(num_nivel = '9','1','0') flag_ultimo_nivel
FROM bi_corp_staging.rio157_ms0_dm_je_dwh_prod_gestion_ctr
WHERE partition_date = '2020-08-14'
"