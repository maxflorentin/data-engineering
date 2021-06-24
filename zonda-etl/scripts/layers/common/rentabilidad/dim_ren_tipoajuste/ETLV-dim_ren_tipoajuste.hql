"
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT
	OVERWRITE TABLE bi_corp_common.dim_ren_tipoajuste
SELECT
	TA.cod_pais   cod_ren_pais,
	TA.cod_tip_ajuste   cod_ren_tipo_ajuste,
	IF(TA.cod_tip_ajuste_niv_1 ='null',NULL,TA.cod_tip_ajuste_niv_1 )  cod_ren_tipo_ajuste_niv_1,
	IF(TA.cod_tip_ajuste_niv_2 ='null',NULL,TA.cod_tip_ajuste_niv_2 )  cod_ren_tipo_ajuste_niv_2,
	IF(TA.cod_tip_ajuste_niv_3 ='null',NULL,TA.cod_tip_ajuste_niv_3 )  cod_ren_tipo_ajuste_niv_3,
	IF(TA.cod_tip_ajuste_niv_4 ='null',NULL,TA.cod_tip_ajuste_niv_4 )  cod_ren_tipo_ajuste_niv_4,
	IF(TA.cod_tip_ajuste_niv_5 ='null',NULL,TA.cod_tip_ajuste_niv_5 )  cod_ren_tipo_ajuste_niv_5,
	IF(TA.cod_tip_ajuste_niv_6 ='null',NULL,TA.cod_tip_ajuste_niv_6 )  cod_ren_tipo_ajuste_niv_6,
	IF(TA.cod_tip_ajuste_niv_7 ='null',NULL,TA.cod_tip_ajuste_niv_7 )  cod_ren_tipo_ajuste_niv_7,
	IF(TA.cod_tip_ajuste_niv_8 ='null',NULL,TA.cod_tip_ajuste_niv_8 )  cod_ren_tipo_ajuste_niv_8,
	IF(TA.cod_tip_ajuste_niv_9 ='null',NULL,TA.cod_tip_ajuste_niv_9 )  cod_ren_tipo_ajuste_niv_9,
	IF(TA.cod_tip_ajuste_niv_10 ='null',NULL,TA.cod_tip_ajuste_niv_10 )  cod_ren_tipo_ajuste_niv_10,
	IF(TA.cod_tip_ajuste_niv_11 ='null',NULL,TA.cod_tip_ajuste_niv_11 )  cod_ren_tipo_ajuste_niv_11,
	IF(TA.cod_tip_ajuste_niv_12 ='null',NULL,TA.cod_tip_ajuste_niv_12 )  cod_ren_tipo_ajuste_niv_12,
	IF(TA.cod_tip_ajuste_niv_13 ='null',NULL,TA.cod_tip_ajuste_niv_13 )  cod_ren_tipo_ajuste_niv_13,
	IF(TA.cod_tip_ajuste_niv_14 ='null',NULL,TA.cod_tip_ajuste_niv_14 )  cod_ren_tipo_ajuste_niv_14,
	IF(TA.des_tip_ajuste_niv_1 ='null',NULL,TA.des_tip_ajuste_niv_1 )  ds_ren_tipo_ajuste_niv_1,
	IF(TA.des_tip_ajuste_niv_2 ='null',NULL,TA.des_tip_ajuste_niv_2 )  ds_ren_tipo_ajuste_niv_2,
	IF(TA.des_tip_ajuste_niv_3 ='null',NULL,TA.des_tip_ajuste_niv_3 )  ds_ren_tipo_ajuste_niv_3,
	IF(TA.des_tip_ajuste_niv_4 ='null',NULL,TA.des_tip_ajuste_niv_4 )  ds_ren_tipo_ajuste_niv_4,
	IF(TA.des_tip_ajuste_niv_5 ='null',NULL,TA.des_tip_ajuste_niv_5 )  ds_ren_tipo_ajuste_niv_5,
	IF(TA.des_tip_ajuste_niv_6 ='null',NULL,TA.des_tip_ajuste_niv_6 )  ds_ren_tipo_ajuste_niv_6,
	IF(TA.des_tip_ajuste_niv_7 ='null',NULL,TA.des_tip_ajuste_niv_7 )  ds_ren_tipo_ajuste_niv_7,
	IF(TA.des_tip_ajuste_niv_8 ='null',NULL,TA.des_tip_ajuste_niv_8 )  ds_ren_tipo_ajuste_niv_8,
	IF(TA.des_tip_ajuste_niv_9 ='null',NULL,TA.des_tip_ajuste_niv_9 )  ds_ren_tipo_ajuste_niv_9,
	IF(TA.des_tip_ajuste_niv_10 ='null',NULL,TA.des_tip_ajuste_niv_10 )  ds_ren_tipo_ajuste_niv_10,
	IF(TA.des_tip_ajuste_niv_11 ='null',NULL,TA.des_tip_ajuste_niv_11 )  ds_ren_tipo_ajuste_niv_11,
	IF(TA.des_tip_ajuste_niv_12 ='null',NULL,TA.des_tip_ajuste_niv_12 )  ds_ren_tipo_ajuste_niv_12,
	IF(TA.des_tip_ajuste_niv_13 ='null',NULL,TA.des_tip_ajuste_niv_13 )  ds_ren_tipo_ajuste_niv_13,
	IF(TA.des_tip_ajuste_niv_14 ='null',NULL,TA.des_tip_ajuste_niv_14 )  ds_ren_tipo_ajuste_niv_14,
	TA.partition_date   partition_date
FROM
	bi_corp_staging.rio157_ms0_dm_dwh_tipo_ajuste_ TA
WHERE
	TA.partition_date  in (SELECT max(partition_date) FROM bi_corp_staging.rio157_ms0_dm_dwh_tipo_ajuste_);
"