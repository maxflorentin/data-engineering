"
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT
	OVERWRITE TABLE bi_corp_common.dim_ren_gruposglobales
SELECT
	GG.cod_pais   cod_ren_pais,
	GG.cod_grupo_cliente   cod_ren_grupo_cliente,
	IF(GG.cod_grupo_cliente_niv_1 ='null',NULL,GG.cod_grupo_cliente_niv_1 )  cod_ren_grupo_cliente_niv_1,
	IF(GG.cod_grupo_cliente_niv_2 ='null',NULL,GG.cod_grupo_cliente_niv_2 )  cod_ren_grupo_cliente_niv_2,
	IF(GG.cod_grupo_cliente_niv_3 ='null',NULL,GG.cod_grupo_cliente_niv_3 )  cod_ren_grupo_cliente_niv_3,
	IF(GG.cod_grupo_cliente_niv_4 ='null',NULL,GG.cod_grupo_cliente_niv_4 )  cod_ren_grupo_cliente_niv_4,
	IF(GG.cod_grupo_cliente_niv_5 ='null',NULL,GG.cod_grupo_cliente_niv_5 )  cod_ren_grupo_cliente_niv_5,
	IF(GG.cod_grupo_cliente_niv_6 ='null',NULL,GG.cod_grupo_cliente_niv_6 )  cod_ren_grupo_cliente_niv_6,
	IF(GG.cod_grupo_cliente_niv_7 ='null',NULL,GG.cod_grupo_cliente_niv_7 )  cod_ren_grupo_cliente_niv_7,
	IF(GG.cod_grupo_cliente_niv_8 ='null',NULL,GG.cod_grupo_cliente_niv_8 )  cod_ren_grupo_cliente_niv_8,
	IF(GG.cod_grupo_cliente_niv_9 ='null',NULL,GG.cod_grupo_cliente_niv_9 )  cod_ren_grupo_cliente_niv_9,
	IF(GG.cod_grupo_cliente_niv_10 ='null',NULL,GG.cod_grupo_cliente_niv_10 )  cod_ren_grupo_cliente_niv_10,
	IF(GG.cod_grupo_cliente_niv_11 ='null',NULL,GG.cod_grupo_cliente_niv_11 )  cod_ren_grupo_cliente_niv_11,
	IF(GG.cod_grupo_cliente_niv_12 ='null',NULL,GG.cod_grupo_cliente_niv_12 )  cod_ren_grupo_cliente_niv_12,
	IF(GG.cod_grupo_cliente_niv_13 ='null',NULL,GG.cod_grupo_cliente_niv_13 )  cod_ren_grupo_cliente_niv_13,
	IF(GG.cod_grupo_cliente_niv_14 ='null',NULL,GG.cod_grupo_cliente_niv_14 )  cod_ren_grupo_cliente_niv_14,
	IF(GG.des_grupo_cliente_niv_1 ='null',NULL,GG.des_grupo_cliente_niv_1 )  ds_ren_grupo_cliente_niv_1,
	IF(GG.des_grupo_cliente_niv_2 ='null',NULL,GG.des_grupo_cliente_niv_2 )  ds_ren_grupo_cliente_niv_2,
	IF(GG.des_grupo_cliente_niv_3 ='null',NULL,GG.des_grupo_cliente_niv_3 )  ds_ren_grupo_cliente_niv_3,
	IF(GG.des_grupo_cliente_niv_4 ='null',NULL,GG.des_grupo_cliente_niv_4 )  ds_ren_grupo_cliente_niv_4,
	IF(GG.des_grupo_cliente_niv_5 ='null',NULL,GG.des_grupo_cliente_niv_5 )  ds_ren_grupo_cliente_niv_5,
	IF(GG.des_grupo_cliente_niv_6 ='null',NULL,GG.des_grupo_cliente_niv_6 )  ds_ren_grupo_cliente_niv_6,
	IF(GG.des_grupo_cliente_niv_7 ='null',NULL,GG.des_grupo_cliente_niv_7 )  ds_ren_grupo_cliente_niv_7,
	IF(GG.des_grupo_cliente_niv_8 ='null',NULL,GG.des_grupo_cliente_niv_8 )  ds_ren_grupo_cliente_niv_8,
	IF(GG.des_grupo_cliente_niv_9 ='null',NULL,GG.des_grupo_cliente_niv_9 )  ds_ren_grupo_cliente_niv_9,
	IF(GG.des_grupo_cliente_niv_10 ='null',NULL,GG.des_grupo_cliente_niv_10 )  ds_ren_grupo_cliente_niv_10,
	IF(GG.des_grupo_cliente_niv_11 ='null',NULL,GG.des_grupo_cliente_niv_11 )  ds_ren_grupo_cliente_niv_11,
	IF(GG.des_grupo_cliente_niv_12 ='null',NULL,GG.des_grupo_cliente_niv_12 )  ds_ren_grupo_cliente_niv_12,
	IF(GG.des_grupo_cliente_niv_13 ='null',NULL,GG.des_grupo_cliente_niv_13 )  ds_ren_grupo_cliente_niv_13,
	IF(GG.des_grupo_cliente_niv_14 ='null',NULL,GG.des_grupo_cliente_niv_14 )  ds_ren_grupo_cliente_niv_14,
	GG.partition_date   partition_date
FROM
	bi_corp_staging.rio157_ms0_dm_dwh_grupos_globales_ GG
WHERE
	GG.partition_date in (SELECT max(partition_date) FROM bi_corp_staging.rio157_ms0_dm_dwh_grupos_globales_);
"