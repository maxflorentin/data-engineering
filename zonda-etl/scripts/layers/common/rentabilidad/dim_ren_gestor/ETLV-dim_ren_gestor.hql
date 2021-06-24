"
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT
	OVERWRITE TABLE bi_corp_common.dim_ren_gestor

SELECT
	ge.cod_pais   cod_ren_pais,
	ge.cod_gestor cod_ren_gestor,
	IF(ge.cod_gestor_niv_1 ='null',NULL,ge.cod_gestor_niv_1 )  cod_ren_gestor_niv_1,
	IF(ge.cod_gestor_niv_2 ='null',NULL,ge.cod_gestor_niv_2 )  cod_ren_gestor_niv_2,
	IF(ge.cod_gestor_niv_3 ='null',NULL,ge.cod_gestor_niv_3 )  cod_ren_gestor_niv_3,
	IF(ge.cod_gestor_niv_4 ='null',NULL,ge.cod_gestor_niv_4 )  cod_ren_gestor_niv_4,
	IF(ge.cod_gestor_niv_5 ='null',NULL,ge.cod_gestor_niv_5 )  cod_ren_gestor_niv_5,
	IF(ge.cod_gestor_niv_6 ='null',NULL,ge.cod_gestor_niv_6 )  cod_ren_gestor_niv_6,
	IF(ge.cod_gestor_niv_7 ='null',NULL,ge.cod_gestor_niv_7 )  cod_ren_gestor_niv_7,
	IF(ge.cod_gestor_niv_8 ='null',NULL,ge.cod_gestor_niv_8 )  cod_ren_gestor_niv_8,
	IF(ge.cod_gestor_niv_9 ='null',NULL,ge.cod_gestor_niv_9 )  cod_ren_gestor_niv_9,
	IF(ge.cod_gestor_niv_10 ='null',NULL,ge.cod_gestor_niv_10 )  cod_ren_gestor_niv_10,
	IF(ge.cod_gestor_niv_11 ='null',NULL,ge.cod_gestor_niv_11 )  cod_ren_gestor_niv_11,
	IF(ge.cod_gestor_niv_12 ='null',NULL,ge.cod_gestor_niv_12 )  cod_ren_gestor_niv_12,
	IF(ge.cod_gestor_niv_13 ='null',NULL,ge.cod_gestor_niv_13 )  cod_ren_gestor_niv_13,
	IF(ge.cod_gestor_niv_14 ='null',NULL,ge.cod_gestor_niv_14 )  cod_ren_gestor_niv_14,
	IF(ge.des_gestor_niv_1 ='null',NULL,ge.des_gestor_niv_1 )  ds_ren_gestor_niv_1,
	IF(ge.des_gestor_niv_2 ='null',NULL,ge.des_gestor_niv_2 )  ds_ren_gestor_niv_2,
	IF(ge.des_gestor_niv_3 ='null',NULL,ge.des_gestor_niv_3 )  ds_ren_gestor_niv_3,
	IF(ge.des_gestor_niv_4 ='null',NULL,ge.des_gestor_niv_4 )  ds_ren_gestor_niv_4,
	IF(ge.des_gestor_niv_5 ='null',NULL,ge.des_gestor_niv_5 )  ds_ren_gestor_niv_5,
	IF(ge.des_gestor_niv_6 ='null',NULL,ge.des_gestor_niv_6 )  ds_ren_gestor_niv_6,
	IF(ge.des_gestor_niv_7 ='null',NULL,ge.des_gestor_niv_7 )  ds_ren_gestor_niv_7,
	IF(ge.des_gestor_niv_8 ='null',NULL,ge.des_gestor_niv_8 )  ds_ren_gestor_niv_8,
	IF(ge.des_gestor_niv_9 ='null',NULL,ge.des_gestor_niv_9 )  ds_ren_gestor_niv_9,
	IF(ge.des_gestor_niv_10 ='null',NULL,ge.des_gestor_niv_10 )  ds_ren_gestor_niv_10,
	IF(ge.des_gestor_niv_11 ='null',NULL,ge.des_gestor_niv_11 )  ds_ren_gestor_niv_11,
	IF(ge.des_gestor_niv_12 ='null',NULL,ge.des_gestor_niv_12 )  ds_ren_gestor_niv_12,
	IF(ge.des_gestor_niv_13 ='null',NULL,ge.des_gestor_niv_13 )  ds_ren_gestor_niv_13,
	IF(ge.des_gestor_niv_14 ='null',NULL,ge.des_gestor_niv_14 )  ds_ren_gestor_niv_14,
	ge.partition_date   partition_date
FROM
	bi_corp_staging.rio157_ms0_dm_dwh_gestor_ ge
WHERE ge.partition_date in (SELECT max(partition_date) FROM bi_corp_staging.rio157_ms0_dm_dwh_gestor_);
"