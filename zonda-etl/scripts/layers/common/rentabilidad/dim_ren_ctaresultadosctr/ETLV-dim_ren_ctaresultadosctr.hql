"
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT
	OVERWRITE TABLE bi_corp_common.dim_ren_ctaresultadosctr
SELECT
	cr.cod_pais   cod_ren_cod_pais,
	cr.cod_cta_resultados   cod_ren_cta_resultados,
	IF(cr.cod_cta_resultados_niv_1 ='null',NULL,cr.cod_cta_resultados_niv_1 )  cod_ren_cta_resultados_niv_1,
	IF(cr.cod_cta_resultados_niv_2 ='null',NULL,cr.cod_cta_resultados_niv_2 )  cod_ren_cta_resultados_niv_2,
	IF(cr.cod_cta_resultados_niv_3 ='null',NULL,cr.cod_cta_resultados_niv_3 )  cod_ren_cta_resultados_niv_3,
	IF(cr.cod_cta_resultados_niv_4 ='null',NULL,cr.cod_cta_resultados_niv_4 )  cod_ren_cta_resultados_niv_4,
	IF(cr.cod_cta_resultados_niv_5 ='null',NULL,cr.cod_cta_resultados_niv_5 )  cod_ren_cta_resultados_niv_5,
	IF(cr.cod_cta_resultados_niv_6 ='null',NULL,cr.cod_cta_resultados_niv_6 )  cod_ren_cta_resultados_niv_6,
	IF(cr.cod_cta_resultados_niv_7 ='null',NULL,cr.cod_cta_resultados_niv_7 )  cod_ren_cta_resultados_niv_7,
	IF(cr.cod_cta_resultados_niv_8 ='null',NULL,cr.cod_cta_resultados_niv_8 )  cod_ren_cta_resultados_niv_8,
	IF(cr.cod_cta_resultados_niv_9 ='null',NULL,cr.cod_cta_resultados_niv_9 )  cod_ren_cta_resultados_niv_9,
	IF(cr.cod_cta_resultados_niv_10 ='null',NULL,cr.cod_cta_resultados_niv_10 )  cod_ren_cta_resultados_niv_10,
	IF(cr.cod_cta_resultados_niv_11 ='null',NULL,cr.cod_cta_resultados_niv_11 )  cod_ren_cta_resultados_niv_11,
	IF(cr.cod_cta_resultados_niv_12 ='null',NULL,cr.cod_cta_resultados_niv_12 )  cod_ren_cta_resultados_niv_12,
	IF(cr.cod_cta_resultados_niv_13 ='null',NULL,cr.cod_cta_resultados_niv_13 )  cod_ren_cta_resultados_niv_13,
	IF(cr.cod_cta_resultados_niv_14 ='null',NULL,cr.cod_cta_resultados_niv_14 )  cod_ren_cta_resultados_niv_14,
	IF(cr.des_cta_resultados_niv_1 ='null',NULL,cr.des_cta_resultados_niv_1 )  ds_ren_cta_resultados_niv_1,
	IF(cr.des_cta_resultados_niv_2 ='null',NULL,cr.des_cta_resultados_niv_2 )  ds_ren_cta_resultados_niv_2,
	IF(cr.des_cta_resultados_niv_3 ='null',NULL,cr.des_cta_resultados_niv_3 )  ds_ren_cta_resultados_niv_3,
	IF(cr.des_cta_resultados_niv_4 ='null',NULL,cr.des_cta_resultados_niv_4 )  ds_ren_cta_resultados_niv_4,
	IF(cr.des_cta_resultados_niv_5 ='null',NULL,cr.des_cta_resultados_niv_5 )  ds_ren_cta_resultados_niv_5,
	IF(cr.des_cta_resultados_niv_6 ='null',NULL,cr.des_cta_resultados_niv_6 )  ds_ren_cta_resultados_niv_6,
	IF(cr.des_cta_resultados_niv_7 ='null',NULL,cr.des_cta_resultados_niv_7 )  ds_ren_cta_resultados_niv_7,
	IF(cr.des_cta_resultados_niv_8 ='null',NULL,cr.des_cta_resultados_niv_8 )  ds_ren_cta_resultados_niv_8,
	IF(cr.des_cta_resultados_niv_9 ='null',NULL,cr.des_cta_resultados_niv_9 )  ds_ren_cta_resultados_niv_9,
	IF(cr.des_cta_resultados_niv_10 ='null',NULL,cr.des_cta_resultados_niv_10 )  ds_ren_cta_resultados_niv_10,
	IF(cr.des_cta_resultados_niv_11 ='null',NULL,cr.des_cta_resultados_niv_11 )  ds_ren_cta_resultados_niv_11,
	IF(cr.des_cta_resultados_niv_12 ='null',NULL,cr.des_cta_resultados_niv_12 )  ds_ren_cta_resultados_niv_12,
	IF(cr.des_cta_resultados_niv_13 ='null',NULL,cr.des_cta_resultados_niv_13 )  ds_ren_cta_resultados_niv_13,
	IF(cr.des_cta_resultados_niv_14 ='null',NULL,cr.des_cta_resultados_niv_14 )  ds_ren_cta_resultados_niv_14,
	cr.partition_date   partition_date
FROM
	bi_corp_staging.rio157_ms0_dm_dwh_cta_resultados_ctr cr
WHERE
	UPPER(cr.COD_JERQ_CR01) = 'JCNBV'
	AND cr.partition_date in (SELECT max(partition_date) FROM bi_corp_staging.rio157_ms0_dm_dwh_cta_resultados_ctr);
"