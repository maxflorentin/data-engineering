set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;
-----------------------------------------------
INSERT OVERWRITE TABLE bi_corp_common.stk_cys_mapaindividuosnegocios
PARTITION(partition_date)
SELECT TRIM(fechacarga) ds_cys_periodo
	, CAST(nup AS BIGINT) cod_per_nup 
	, CAST(decil AS INT) int_cys_decil
	, IF(TRIM(marca) = 'IND', 1, 0) flag_cys_individuo
	, IF(TRIM(marca) = 'PF', 1, 0) flag_cys_pfpymes
	, last_day(to_date(CONCAT(SUBSTRING(fechacarga,1, 4),'-',SUBSTRING(fechacarga,5, 2),'-01'))) partition_date
FROM bi_corp_risk.mapaindividuosnegocios ;