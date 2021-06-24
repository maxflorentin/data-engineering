"
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT
	OVERWRITE TABLE bi_corp_common.dim_ren_centro
SELECT
	c.cod_pais   cod_ren_pais,
	c.cod_centro_cont   cod_ren_centro_cont,
	IF(c.cod_centro_cont_niv_1 ='null',NULL,c.cod_centro_cont_niv_1 )  cod_ren_centro_cont_niv_1,
	IF(c.cod_centro_cont_niv_2 ='null',NULL,c.cod_centro_cont_niv_2 )  cod_ren_centro_cont_niv_2,
	IF(c.cod_centro_cont_niv_3 ='null',NULL,c.cod_centro_cont_niv_3 )  cod_ren_centro_cont_niv_3,
	IF(c.cod_centro_cont_niv_4 ='null',NULL,c.cod_centro_cont_niv_4 )  cod_ren_centro_cont_niv_4,
	IF(c.cod_centro_cont_niv_5 ='null',NULL,c.cod_centro_cont_niv_5 )  cod_ren_centro_cont_niv_5,
	IF(c.cod_centro_cont_niv_6 ='null',NULL,c.cod_centro_cont_niv_6 )  cod_ren_centro_cont_niv_6,
	IF(c.cod_centro_cont_niv_7 ='null',NULL,c.cod_centro_cont_niv_7 )  cod_ren_centro_cont_niv_7,
	IF(c.cod_centro_cont_niv_8 ='null',NULL,c.cod_centro_cont_niv_8 )  cod_ren_centro_cont_niv_8,
	IF(c.cod_centro_cont_niv_9 ='null',NULL,c.cod_centro_cont_niv_9 )  cod_ren_centro_cont_niv_9,
	IF(c.cod_centro_cont_niv_10 ='null',NULL,c.cod_centro_cont_niv_10 )  cod_ren_centro_cont_niv_10,
	IF(c.cod_centro_cont_niv_11 ='null',NULL,c.cod_centro_cont_niv_11 )  cod_ren_centro_cont_niv_11,
	IF(c.cod_centro_cont_niv_12 ='null',NULL,c.cod_centro_cont_niv_12 )  cod_ren_centro_cont_niv_12,
	IF(c.cod_centro_cont_niv_13 ='null',NULL,c.cod_centro_cont_niv_13 )  cod_ren_centro_cont_niv_13,
	IF(c.cod_centro_cont_niv_14 ='null',NULL,c.cod_centro_cont_niv_14 )  cod_ren_centro_cont_niv_14,
	IF(c.des_centro_cont_niv_1 ='null',NULL,c.des_centro_cont_niv_1 )  ds_ren_centro_cont_niv_1,
	IF(c.des_centro_cont_niv_2 ='null',NULL,c.des_centro_cont_niv_2 )  ds_ren_centro_cont_niv_2,
	IF(c.des_centro_cont_niv_3 ='null',NULL,c.des_centro_cont_niv_3 )  ds_ren_centro_cont_niv_3,
	IF(c.des_centro_cont_niv_4 ='null',NULL,c.des_centro_cont_niv_4 )  ds_ren_centro_cont_niv_4,
	IF(c.des_centro_cont_niv_5 ='null',NULL,c.des_centro_cont_niv_5 )  ds_ren_centro_cont_niv_5,
	IF(c.des_centro_cont_niv_6 ='null',NULL,c.des_centro_cont_niv_6 )  ds_ren_centro_cont_niv_6,
	IF(c.des_centro_cont_niv_7 ='null',NULL,c.des_centro_cont_niv_7 )  ds_ren_centro_cont_niv_7,
	IF(c.des_centro_cont_niv_8 ='null',NULL,c.des_centro_cont_niv_8 )  ds_ren_centro_cont_niv_8,
	IF(c.des_centro_cont_niv_9 ='null',NULL,c.des_centro_cont_niv_9 )  ds_ren_centro_cont_niv_9,
	IF(c.des_centro_cont_niv_10 ='null',NULL,c.des_centro_cont_niv_10 )  ds_ren_centro_cont_niv_10,
	IF(c.des_centro_cont_niv_11 ='null',NULL,c.des_centro_cont_niv_11 )  ds_ren_centro_cont_niv_11,
	IF(c.des_centro_cont_niv_12 ='null',NULL,c.des_centro_cont_niv_12 )  ds_ren_centro_cont_niv_12,
	IF(c.des_centro_cont_niv_13 ='null',NULL,c.des_centro_cont_niv_13 )  ds_ren_centro_cont_niv_13,
	IF(c.des_centro_cont_niv_14 ='null',NULL,c.des_centro_cont_niv_14 )  ds_ren_centro_cont_niv_14,
	c.partition_date   partition_date
FROM
	bi_corp_staging.rio157_ms0_dm_dwh_centro_ c
WHERE
	c.partition_date in (SELECT max(partition_date) FROM bi_corp_staging.rio157_ms0_dm_dwh_centro_);
"