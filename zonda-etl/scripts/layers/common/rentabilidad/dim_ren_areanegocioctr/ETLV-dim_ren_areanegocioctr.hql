"
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT
	OVERWRITE TABLE bi_corp_common.dim_ren_areanegocioctr

SELECT
	an.cod_pais   cod_ren_pais,
	an.cod_area_negocio cod_ren_area_negocio,
	IF(an.cod_area_negocio_niv_1 ='null',NULL,an.cod_area_negocio_niv_1 )  cod_ren_area_negocio_niv_1,
	IF(an.cod_area_negocio_niv_2 ='null',NULL,an.cod_area_negocio_niv_2 )  cod_ren_area_negocio_niv_2,
	IF(an.cod_area_negocio_niv_3 ='null',NULL,an.cod_area_negocio_niv_3 )  cod_ren_area_negocio_niv_3,
	IF(an.cod_area_negocio_niv_4 ='null',NULL,an.cod_area_negocio_niv_4 )  cod_ren_area_negocio_niv_4,
	IF(an.cod_area_negocio_niv_5 ='null',NULL,an.cod_area_negocio_niv_5 )  cod_ren_area_negocio_niv_5,
	IF(an.cod_area_negocio_niv_6 ='null',NULL,an.cod_area_negocio_niv_6 )  cod_ren_area_negocio_niv_6,
	IF(an.cod_area_negocio_niv_7 ='null',NULL,an.cod_area_negocio_niv_7 )  cod_ren_area_negocio_niv_7,
	IF(an.cod_area_negocio_niv_8 ='null',NULL,an.cod_area_negocio_niv_8 )  cod_ren_area_negocio_niv_8,
	IF(an.cod_area_negocio_niv_9 ='null',NULL,an.cod_area_negocio_niv_9 )  cod_ren_area_negocio_niv_9,
	IF(an.cod_area_negocio_niv_10 ='null',NULL,an.cod_area_negocio_niv_10 )  cod_ren_area_negocio_niv_10,
	IF(an.cod_area_negocio_niv_11 ='null',NULL,an.cod_area_negocio_niv_11 )  cod_ren_area_negocio_niv_11,
	IF(an.cod_area_negocio_niv_12 ='null',NULL,an.cod_area_negocio_niv_12 )  cod_ren_area_negocio_niv_12,
	IF(an.cod_area_negocio_niv_13 ='null',NULL,an.cod_area_negocio_niv_13 )  cod_ren_area_negocio_niv_13,
	IF(an.cod_area_negocio_niv_14 ='null',NULL,an.cod_area_negocio_niv_14 )  cod_ren_area_negocio_niv_14,
	IF(an.des_area_negocio_niv_1 ='null',NULL,an.des_area_negocio_niv_1 )  ds_ren_area_negocio_niv_1,
	IF(an.des_area_negocio_niv_2 ='null',NULL,an.des_area_negocio_niv_2 )  ds_ren_area_negocio_niv_2,
	IF(an.des_area_negocio_niv_3 ='null',NULL,an.des_area_negocio_niv_3 )  ds_ren_area_negocio_niv_3,
	IF(an.des_area_negocio_niv_4 ='null',NULL,an.des_area_negocio_niv_4 )  ds_ren_area_negocio_niv_4,
	IF(an.des_area_negocio_niv_5 ='null',NULL,an.des_area_negocio_niv_5 )  ds_ren_area_negocio_niv_5,
	IF(an.des_area_negocio_niv_6 ='null',NULL,an.des_area_negocio_niv_6 )  ds_ren_area_negocio_niv_6,
	IF(an.des_area_negocio_niv_7 ='null',NULL,an.des_area_negocio_niv_7 )  ds_ren_area_negocio_niv_7,
	IF(an.des_area_negocio_niv_8 ='null',NULL,an.des_area_negocio_niv_8 )  ds_ren_area_negocio_niv_8,
	IF(an.des_area_negocio_niv_9 ='null',NULL,an.des_area_negocio_niv_9 )  ds_ren_area_negocio_niv_9,
	IF(an.des_area_negocio_niv_10 ='null',NULL,an.des_area_negocio_niv_10 )  ds_ren_area_negocio_niv_10,
	IF(an.des_area_negocio_niv_11 ='null',NULL,an.des_area_negocio_niv_11 )  ds_ren_area_negocio_niv_11,
	IF(an.des_area_negocio_niv_12 ='null',NULL,an.des_area_negocio_niv_12 )  ds_ren_area_negocio_niv_12,
	IF(an.des_area_negocio_niv_13 ='null',NULL,an.des_area_negocio_niv_13 )  ds_ren_area_negocio_niv_13,
	IF(an.des_area_negocio_niv_14 ='null',NULL,an.des_area_negocio_niv_14 )  ds_ren_area_negocio_niv_14,
	an.partition_date   partition_date
FROM
	bi_corp_staging.rio157_ms0_dm_dwh_area_negocio_ctr an
WHERE
	an.cod_jerq_adn01 = 'JAN01'
	AND an.partition_date in (SELECT max(partition_date) FROM bi_corp_staging.rio157_ms0_dm_dwh_area_negocio_ctr);
"