"
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT
	OVERWRITE TABLE bi_corp_common.dim_ren_bcacorp
SELECT
	BC.cod_pais    cod_ren_pais,
	BC.cod_bca_corp   cod_ren_bca_corp,
	IF(BC.cod_bca_corp_niv_1 ='null',NULL,BC.cod_bca_corp_niv_1 )   cod_ren_bca_corp_niv_1,
	IF(BC.cod_bca_corp_niv_2 ='null',NULL,BC.cod_bca_corp_niv_2 )   cod_ren_bca_corp_niv_2,
	IF(BC.cod_bca_corp_niv_3 ='null',NULL,BC.cod_bca_corp_niv_3 )   cod_ren_bca_corp_niv_3,
	IF(BC.cod_bca_corp_niv_4 ='null',NULL,BC.cod_bca_corp_niv_4 )   cod_ren_bca_corp_niv_4,
	IF(BC.cod_bca_corp_niv_5 ='null',NULL,BC.cod_bca_corp_niv_5 )   cod_ren_bca_corp_niv_5,
	IF(BC.cod_bca_corp_niv_6 ='null',NULL,BC.cod_bca_corp_niv_6 )   cod_ren_bca_corp_niv_6,
	IF(BC.cod_bca_corp_niv_7 ='null',NULL,BC.cod_bca_corp_niv_7 )   cod_ren_bca_corp_niv_7,
	IF(BC.cod_bca_corp_niv_8 ='null',NULL,BC.cod_bca_corp_niv_8 )   cod_ren_bca_corp_niv_8,
	IF(BC.cod_bca_corp_niv_9 ='null',NULL,BC.cod_bca_corp_niv_9 )   cod_ren_bca_corp_niv_9,
	IF(BC.cod_bca_corp_niv_10 ='null',NULL,BC.cod_bca_corp_niv_10 )   cod_ren_bca_corp_niv_10,
	IF(BC.cod_bca_corp_niv_11 ='null',NULL,BC.cod_bca_corp_niv_11 )   cod_ren_bca_corp_niv_11,
	IF(BC.cod_bca_corp_niv_12 ='null',NULL,BC.cod_bca_corp_niv_12 )   cod_ren_bca_corp_niv_12,
	IF(BC.cod_bca_corp_niv_13 ='null',NULL,BC.cod_bca_corp_niv_13 )   cod_ren_bca_corp_niv_13,
	IF(BC.cod_bca_corp_niv_14 ='null',NULL,BC.cod_bca_corp_niv_14 )   cod_ren_bca_corp_niv_14,
	IF(BC.des_bca_corp_niv_1 ='null',NULL,BC.des_bca_corp_niv_1 )   ds_ren_bca_corp_niv_1,
	IF(BC.des_bca_corp_niv_2 ='null',NULL,BC.des_bca_corp_niv_2 )   ds_ren_bca_corp_niv_2,
	IF(BC.des_bca_corp_niv_3 ='null',NULL,BC.des_bca_corp_niv_3 )   ds_ren_bca_corp_niv_3,
	IF(BC.des_bca_corp_niv_4 ='null',NULL,BC.des_bca_corp_niv_4 )   ds_ren_bca_corp_niv_4,
	IF(BC.des_bca_corp_niv_5 ='null',NULL,BC.des_bca_corp_niv_5 )   ds_ren_bca_corp_niv_5,
	IF(BC.des_bca_corp_niv_6 ='null',NULL,BC.des_bca_corp_niv_6 )   ds_ren_bca_corp_niv_6,
	IF(BC.des_bca_corp_niv_7 ='null',NULL,BC.des_bca_corp_niv_7 )   ds_ren_bca_corp_niv_7,
	IF(BC.des_bca_corp_niv_8 ='null',NULL,BC.des_bca_corp_niv_8 )   ds_ren_bca_corp_niv_8,
	IF(BC.des_bca_corp_niv_9 ='null',NULL,BC.des_bca_corp_niv_9 )   ds_ren_bca_corp_niv_10,
	IF(BC.des_bca_corp_niv_10 ='null',NULL,BC.des_bca_corp_niv_10 )   ds_ren_bca_corp_niv_11,
	IF(BC.des_bca_corp_niv_11 ='null',NULL,BC.des_bca_corp_niv_11 )   ds_ren_bca_corp_niv_12,
	IF(BC.des_bca_corp_niv_12 ='null',NULL,BC.des_bca_corp_niv_12 )   ds_ren_bca_corp_niv_13,
	IF(BC.des_bca_corp_niv_13 ='null',NULL,BC.des_bca_corp_niv_13 )   ds_ren_bca_corp_niv_14,
	IF(BC.des_bca_corp_niv_14 ='null',NULL,BC.des_bca_corp_niv_14 )   ds_ren_bca_corp_niv_15,
	BC.partition_date    partition_date
FROM
	bi_corp_staging.rio157_ms0_dm_dwh_bca_corp_ BC
WHERE
	BC.partition_date in (SELECT max(partition_date) FROM bi_corp_staging.rio157_ms0_dm_dwh_bca_corp_);
"