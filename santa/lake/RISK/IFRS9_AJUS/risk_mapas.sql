SELECT * FROM bi_corp_risk.mapaindividuosnegocios

-- DROP TABLE bi_corp_common.stk_cys_puntajesseguimiento ;

CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_cys_mapaindividuosnegocios (
	ds_cys_periodo	string ,
	cod_per_nup	int ,
	int_cys_decil int ,
	flag_cys_individuo int ,
	flag_cys_pfpymes int )
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/santander/bi-corp/common/cys/stk_cys_mapaindividuosnegocios'

set hive.execution.engine=spark;
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;
-----------------------------------------------
INSERT OVERWRITE TABLE bi_corp_common.stk_cys_mapaindividuosnegocios
PARTITION(partition_date)
SELECT TRIM(fechacarga) ds_cys_periodo
	, CAST(nup AS INT) cod_per_nup 
	, CAST(decil AS INT) int_cys_decil
	, IF(TRIM(marca) = 'IND', 1, 0) flag_cys_individuo
	, IF(TRIM(marca) = 'PF', 1, 0) flag_cys_pfpymes
	, last_day(to_date(CONCAT(SUBSTRING(fechacarga,1, 4),'-',SUBSTRING(fechacarga,5, 2),'-01'))) partition_date
FROM bi_corp_risk.mapaindividuosnegocios ;

SELECT * FROM bi_corp_common.stk_cys_mapaindividuosnegocios ;



CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_cys_mapapymes (
	ds_cys_periodo	string ,
	cod_per_nup	int ,
	cod_cys_puntajeseguimiento string )
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/santander/bi-corp/common/cys/stk_cys_mapapymes'

set hive.execution.engine=spark;
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;
-----------------------------------------------
INSERT OVERWRITE TABLE bi_corp_common.stk_cys_mapapymes
PARTITION(partition_date)
SELECT TRIM(periodo) ds_cys_periodo 
	, CAST(nup AS INT) cod_per_nup
	, TRIM(categoria) cod_cys_puntajeseguimiento
	, last_day(to_date(CONCAT(SUBSTRING(periodo,1, 4),'-',SUBSTRING(periodo,5, 2),'-01'))) partition_date 
FROM bi_corp_risk.mapa_seguimiento ;

SELECT * FROM bi_corp_common.stk_cys_mapapymes ;

-- santander_business_risk_arda.mapa_seguimiento