set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;
-----------------------------------------------
INSERT OVERWRITE TABLE bi_corp_common.stk_cys_mapapymes
PARTITION(partition_date)
SELECT TRIM(periodo) ds_cys_periodo 
	, CAST(nup AS BIGINT) cod_per_nup
	, TRIM(categoria) cod_cys_puntajeseguimiento
	, last_day(to_date(CONCAT(SUBSTRING(periodo,1, 4),'-',SUBSTRING(periodo,5, 2),'-01'))) partition_date 
FROM bi_corp_risk.mapa_seguimiento ;