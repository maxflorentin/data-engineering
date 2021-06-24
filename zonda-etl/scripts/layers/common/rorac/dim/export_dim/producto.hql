"
set mapred.job.queue.name=root.dataeng ;

SELECT cod_hijo 	
	, des_hijo 
FROM bi_corp_staging.rio157_ms0_dm_je_dwh_prod_gestion_ctr 
WHERE partition_date = '2020-10-09' ;
"