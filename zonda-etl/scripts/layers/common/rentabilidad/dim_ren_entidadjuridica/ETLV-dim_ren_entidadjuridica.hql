"
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT
	OVERWRITE TABLE bi_corp_common.dim_ren_entidadjuridica
SELECT
	ent.cod_pais AS cod_ren_pais,
	ent.cod_entidad_gest AS cod_ren_entidad_gest,
	IF(ent.cod_entidad_gest_niv_1 = 'null', NULL , ent.cod_entidad_gest_niv_1) AS cod_ren_entidad_gest_niv_1,
	IF(ent.cod_entidad_gest_niv_2 = 'null', NULL , ent.cod_entidad_gest_niv_2) AS cod_ren_entidad_gest_niv_2,
	IF(ent.cod_entidad_gest_niv_3 = 'null', NULL , ent.cod_entidad_gest_niv_3) AS cod_ren_entidad_gest_niv_3,
	IF(ent.cod_entidad_gest_niv_4 = 'null', NULL , ent.cod_entidad_gest_niv_4) AS cod_ren_entidad_gest_niv_4,
	IF(ent.cod_entidad_gest_niv_5 = 'null', NULL , ent.cod_entidad_gest_niv_5) AS cod_ren_entidad_gest_niv_5,
	IF(ent.cod_entidad_gest_niv_6 = 'null', NULL , ent.cod_entidad_gest_niv_6) AS cod_ren_entidad_gest_niv_6,
	IF(ent.cod_entidad_gest_niv_7 = 'null', NULL , ent.cod_entidad_gest_niv_7) AS cod_ren_entidad_gest_niv_7,
	IF(ent.cod_entidad_gest_niv_8 = 'null', NULL , ent.cod_entidad_gest_niv_8) AS cod_ren_entidad_gest_niv_8,
	IF(ent.cod_entidad_gest_niv_9 = 'null', NULL , ent.cod_entidad_gest_niv_9) AS cod_ren_entidad_gest_niv_9,
	IF(ent.cod_entidad_gest_niv_10 = 'null', NULL , ent.cod_entidad_gest_niv_10) AS cod_ren_entidad_gest_niv_10,
	IF(ent.cod_entidad_gest_niv_11 = 'null', NULL , ent.cod_entidad_gest_niv_11) AS cod_ren_entidad_gest_niv_11,
	IF(ent.cod_entidad_gest_niv_12 = 'null', NULL , ent.cod_entidad_gest_niv_12) AS cod_ren_entidad_gest_niv_12,
	IF(ent.cod_entidad_gest_niv_13 = 'null', NULL , ent.cod_entidad_gest_niv_13) AS cod_ren_entidad_gest_niv_13,
	IF(ent.cod_entidad_gest_niv_14 = 'null', NULL , ent.cod_entidad_gest_niv_14) AS cod_ren_entidad_gest_niv_14,
	IF(ent.des_entidad_gest_niv_1 = 'null', NULL , ent.des_entidad_gest_niv_1) AS ds_ren_entidad_gest_niv_1,
	IF(ent.des_entidad_gest_niv_2 = 'null', NULL , ent.des_entidad_gest_niv_2) AS ds_ren_entidad_gest_niv_2,
	IF(ent.des_entidad_gest_niv_3 = 'null', NULL , ent.des_entidad_gest_niv_3) AS ds_ren_entidad_gest_niv_3,
	IF(ent.des_entidad_gest_niv_4 = 'null', NULL , ent.des_entidad_gest_niv_4) AS ds_ren_entidad_gest_niv_4,
	IF(ent.des_entidad_gest_niv_5 = 'null', NULL , ent.des_entidad_gest_niv_5) AS ds_ren_entidad_gest_niv_5,
	IF(ent.des_entidad_gest_niv_6 = 'null', NULL , ent.des_entidad_gest_niv_6) AS ds_ren_entidad_gest_niv_6,
	IF(ent.des_entidad_gest_niv_7 = 'null', NULL , ent.des_entidad_gest_niv_7) AS ds_ren_entidad_gest_niv_7,
	IF(ent.des_entidad_gest_niv_8 = 'null', NULL , ent.des_entidad_gest_niv_8) AS ds_ren_entidad_gest_niv_8,
	IF(ent.des_entidad_gest_niv_9 = 'null', NULL , ent.des_entidad_gest_niv_9) AS ds_ren_entidad_gest_niv_9,
	IF(ent.des_entidad_gest_niv_10 = 'null', NULL , ent.des_entidad_gest_niv_10) AS ds_ren_entidad_gest_niv_10,
	IF(ent.des_entidad_gest_niv_11 = 'null', NULL , ent.des_entidad_gest_niv_11) AS ds_ren_entidad_gest_niv_11,
	IF(ent.des_entidad_gest_niv_12 = 'null', NULL , ent.des_entidad_gest_niv_12) AS ds_ren_entidad_gest_niv_12,
	IF(ent.des_entidad_gest_niv_13 = 'null', NULL , ent.des_entidad_gest_niv_13) AS ds_ren_entidad_gest_niv_13,
	IF(ent.des_entidad_gest_niv_14 = 'null', NULL , ent.des_entidad_gest_niv_14) AS ds_ren_entidad_gest_niv_14,
	ent.partition_date AS partition_date
FROM
	bi_corp_staging.rio157_ms0_dm_dwh_entidad_juridica_ ent
WHERE
	ent.cod_jerq_ej01 = 'JEJ01'
	AND ent.partition_date in (SELECT max(partition_date) FROM bi_corp_staging.rio157_ms0_dm_dwh_entidad_juridica_);
"