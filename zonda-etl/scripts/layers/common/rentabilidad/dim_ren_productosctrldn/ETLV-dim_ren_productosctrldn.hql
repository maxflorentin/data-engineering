"
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT
	OVERWRITE TABLE bi_corp_common.dim_ren_productosctrldn
SELECT
	PROD.cod_pais   cod_ren_pais,
	PROD.cod_producto   cod_ren_producto,
	IF(PROD.cod_producto_niv_1 ='null',NULL,PROD.cod_producto_niv_1 )  cod_ren_producto_niv_1,
	IF(PROD.cod_producto_niv_2 ='null',NULL,PROD.cod_producto_niv_2 )  cod_ren_producto_niv_2,
	IF(PROD.cod_producto_niv_3 ='null',NULL,PROD.cod_producto_niv_3 )  cod_ren_producto_niv_3,
	IF(PROD.cod_producto_niv_4 ='null',NULL,PROD.cod_producto_niv_4 )  cod_ren_producto_niv_4,
	IF(PROD.cod_producto_niv_5 ='null',NULL,PROD.cod_producto_niv_5 )  cod_ren_producto_niv_5,
	IF(PROD.cod_producto_niv_6 ='null',NULL,PROD.cod_producto_niv_6 )  cod_ren_producto_niv_6,
	IF(PROD.cod_producto_niv_7 ='null',NULL,PROD.cod_producto_niv_7 )  cod_ren_producto_niv_7,
	IF(PROD.cod_producto_niv_8 ='null',NULL,PROD.cod_producto_niv_8 )  cod_ren_producto_niv_8,
	IF(PROD.cod_producto_niv_9 ='null',NULL,PROD.cod_producto_niv_9 )  cod_ren_producto_niv_9,
	IF(PROD.cod_producto_niv_10 ='null',NULL,PROD.cod_producto_niv_10 )  cod_ren_producto_niv_10,
	IF(PROD.cod_producto_niv_11 ='null',NULL,PROD.cod_producto_niv_11 )  cod_ren_producto_niv_11,
	IF(PROD.cod_producto_niv_12 ='null',NULL,PROD.cod_producto_niv_12 )  cod_ren_producto_niv_12,
	IF(PROD.cod_producto_niv_13 ='null',NULL,PROD.cod_producto_niv_13 )  cod_ren_producto_niv_13,
	IF(PROD.cod_producto_niv_14 ='null',NULL,PROD.cod_producto_niv_14 )  cod_ren_producto_niv_14,
	IF(PROD.des_producto_niv_1 ='null',NULL,PROD.des_producto_niv_1 )  ds_ren_producto_niv_1,
	IF(PROD.des_producto_niv_2 ='null',NULL,PROD.des_producto_niv_2 )  ds_ren_producto_niv_2,
	IF(PROD.des_producto_niv_3 ='null',NULL,PROD.des_producto_niv_3 )  ds_ren_producto_niv_3,
	IF(PROD.des_producto_niv_4 ='null',NULL,PROD.des_producto_niv_4 )  ds_ren_producto_niv_4,
	IF(PROD.des_producto_niv_5 ='null',NULL,PROD.des_producto_niv_5 )  ds_ren_producto_niv_5,
	IF(PROD.des_producto_niv_6 ='null',NULL,PROD.des_producto_niv_6 )  ds_ren_producto_niv_6,
	IF(PROD.des_producto_niv_7 ='null',NULL,PROD.des_producto_niv_7 )  ds_ren_producto_niv_7,
	IF(PROD.des_producto_niv_8 ='null',NULL,PROD.des_producto_niv_8 )  ds_ren_producto_niv_8,
	IF(PROD.des_producto_niv_9 ='null',NULL,PROD.des_producto_niv_9 )  ds_ren_producto_niv_9,
	IF(PROD.des_producto_niv_10 ='null',NULL,PROD.des_producto_niv_10 )  ds_ren_producto_niv_10,
	IF(PROD.des_producto_niv_11 ='null',NULL,PROD.des_producto_niv_11 )  ds_ren_producto_niv_11,
	IF(PROD.des_producto_niv_12 ='null',NULL,PROD.des_producto_niv_12 )  ds_ren_producto_niv_12,
	IF(PROD.des_producto_niv_13 ='null',NULL,PROD.des_producto_niv_13 )  ds_ren_producto_niv_13,
	IF(PROD.des_producto_niv_14 ='null',NULL,PROD.des_producto_niv_14 )  ds_ren_producto_niv_14,
	PROD.signo  ds_ren_producto_signo ,
	PROD.partition_date   partition_date
FROM
	bi_corp_staging.rio157_ms0_dm_dwh_productos_ctr PROD
WHERE
	PROD.cod_jerq_pr01 = 'JBLDN'
	AND PROD.partition_date in (SELECT max(partition_date) FROM bi_corp_staging.rio157_ms0_dm_dwh_productos_ctr);
"