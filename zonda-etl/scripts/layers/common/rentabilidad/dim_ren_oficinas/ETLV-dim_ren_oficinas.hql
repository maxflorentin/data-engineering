"
SET mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT
	OVERWRITE TABLE bi_corp_common.dim_ren_oficinas
SELECT
	offi.cod_pais   cod_ren_pais,
	offi.cod_ofi_comercial   cod_ren_ofi_comercial,
	IF(offi.cod_ofi_comercial_niv_1 ='null',NULL,offi.cod_ofi_comercial_niv_1 )  cod_ren_ofi_comercial_niv_1,
	IF(offi.cod_ofi_comercial_niv_2 ='null',NULL,offi.cod_ofi_comercial_niv_2 )  cod_ren_ofi_comercial_niv_2,
	IF(offi.cod_ofi_comercial_niv_3 ='null',NULL,offi.cod_ofi_comercial_niv_3 )  cod_ren_ofi_comercial_niv_3,
	IF(offi.cod_ofi_comercial_niv_4 ='null',NULL,offi.cod_ofi_comercial_niv_4 )  cod_ren_ofi_comercial_niv_4,
	IF(offi.cod_ofi_comercial_niv_5 ='null',NULL,offi.cod_ofi_comercial_niv_5 )  cod_ren_ofi_comercial_niv_5,
	IF(offi.cod_ofi_comercial_niv_6 ='null',NULL,offi.cod_ofi_comercial_niv_6 )  cod_ren_ofi_comercial_niv_6,
	IF(offi.cod_ofi_comercial_niv_7 ='null',NULL,offi.cod_ofi_comercial_niv_7 )  cod_ren_ofi_comercial_niv_7,
	IF(offi.cod_ofi_comercial_niv_8 ='null',NULL,offi.cod_ofi_comercial_niv_8 )  cod_ren_ofi_comercial_niv_8,
	IF(offi.cod_ofi_comercial_niv_9 ='null',NULL,offi.cod_ofi_comercial_niv_9 )  cod_ren_ofi_comercial_niv_9,
	IF(offi.cod_ofi_comercial_niv_10 ='null',NULL,offi.cod_ofi_comercial_niv_10 )  cod_ren_ofi_comercial_niv_10,
	IF(offi.cod_ofi_comercial_niv_11 ='null',NULL,offi.cod_ofi_comercial_niv_11 )  cod_ren_ofi_comercial_niv_11,
	IF(offi.cod_ofi_comercial_niv_12 ='null',NULL,offi.cod_ofi_comercial_niv_12 )  cod_ren_ofi_comercial_niv_12,
	IF(offi.cod_ofi_comercial_niv_13 ='null',NULL,offi.cod_ofi_comercial_niv_13 )  cod_ren_ofi_comercial_niv_13,
	IF(offi.cod_ofi_comercial_niv_14 ='null',NULL,offi.cod_ofi_comercial_niv_14 )  cod_ren_ofi_comercial_niv_14,
	IF(offi.des_ofi_comercial_niv_1 ='null',NULL,offi.des_ofi_comercial_niv_1 )  ds_ren_ofi_comercial_niv_1,
	IF(offi.des_ofi_comercial_niv_2 ='null',NULL,offi.des_ofi_comercial_niv_2 )  ds_ren_ofi_comercial_niv_2,
	IF(offi.des_ofi_comercial_niv_3 ='null',NULL,offi.des_ofi_comercial_niv_3 )  ds_ren_ofi_comercial_niv_3,
	IF(offi.des_ofi_comercial_niv_4 ='null',NULL,offi.des_ofi_comercial_niv_4 )  ds_ren_ofi_comercial_niv_4,
	IF(offi.des_ofi_comercial_niv_5 ='null',NULL,offi.des_ofi_comercial_niv_5 )  ds_ofi_comercial_niv_5,
	IF(offi.des_ofi_comercial_niv_6 ='null',NULL,offi.des_ofi_comercial_niv_6 )  ds_ren_ofi_comercial_niv_6,
	IF(offi.des_ofi_comercial_niv_7 ='null',NULL,offi.des_ofi_comercial_niv_7 )  ds_ren_ofi_comercial_niv_7,
	IF(offi.des_ofi_comercial_niv_8 ='null',NULL,offi.des_ofi_comercial_niv_8 )  ds_ren_ofi_comercial_niv_8,
	IF(offi.des_ofi_comercial_niv_9 ='null',NULL,offi.des_ofi_comercial_niv_9 )  ds_ren_ofi_comercial_niv_9,
	IF(offi.des_ofi_comercial_niv_10 ='null',NULL,offi.des_ofi_comercial_niv_10 )  ds_ren_ofi_comercial_niv_10,
	IF(offi.des_ofi_comercial_niv_11 ='null',NULL,offi.des_ofi_comercial_niv_11 )  ds_ren_ofi_comercial_niv_11,
	IF(offi.des_ofi_comercial_niv_12 ='null',NULL,offi.des_ofi_comercial_niv_12 )  ds_ren_ofi_comercial_niv_12,
	IF(offi.des_ofi_comercial_niv_13 ='null',NULL,offi.des_ofi_comercial_niv_13 )  ds_ren_ofi_comercial_niv_13,
	IF(offi.des_ofi_comercial_niv_14 ='null',NULL,offi.des_ofi_comercial_niv_14 )  ds_ren_ofi_comercial_niv_14,
	offi.partition_date   partition_date
FROM
	bi_corp_staging.rio157_ms0_dm_dwh_oficina offi
WHERE
	offi.cod_jerq_oc01 = 'JOC01'
	AND offi.partition_date in (SELECT max(partition_date) FROM bi_corp_staging.rio157_ms0_dm_dwh_oficina);
"