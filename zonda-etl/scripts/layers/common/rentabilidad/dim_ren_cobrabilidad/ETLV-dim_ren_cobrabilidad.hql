"
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT
	OVERWRITE TABLE bi_corp_common.dim_ren_cobrabilidad
SELECT
	C.cod_pais cod_ren_pais,
	C.cod_cobrabilidad cod_ren_cobrabilidad,
	C.cod_cobrabilidad_niv_1 cod_ren_cobrabilidad_niv_1,
	C.cod_cobrabilidad_niv_2 cod_ren_cobrabilidad_niv_2,
	C.cod_cobrabilidad_niv_3 cod_ren_cobrabilidad_niv_3,
	C.cod_cobrabilidad_niv_4 cod_ren_cobrabilidad_niv_4,
	C.cod_cobrabilidad_niv_5 cod_ren_cobrabilidad_niv_5,
	C.cod_cobrabilidad_niv_6 cod_ren_cobrabilidad_niv_6,
	C.cod_cobrabilidad_niv_7 cod_ren_cobrabilidad_niv_7,
	C.cod_cobrabilidad_niv_8 cod_ren_cobrabilidad_niv_8,
	C.cod_cobrabilidad_niv_9 cod_ren_cobrabilidad_niv_9,
	C.cod_cobrabilidad_niv_10 cod_ren_cobrabilidad_niv_10,
	C.cod_cobrabilidad_niv_11 cod_ren_cobrabilidad_niv_11,
	C.cod_cobrabilidad_niv_12 cod_ren_cobrabilidad_niv_12,
	C.cod_cobrabilidad_niv_13 cod_ren_cobrabilidad_niv_13,
	C.cod_cobrabilidad_niv_14 cod_ren_cobrabilidad_niv_14,
	C.des_cobrabilidad_niv_1 ds_ren_cobrabilidad_niv_1,
	C.des_cobrabilidad_niv_2 ds_ren_cobrabilidad_niv_2,
	C.des_cobrabilidad_niv_3 ds_ren_cobrabilidad_niv_3,
	C.des_cobrabilidad_niv_4 ds_ren_cobrabilidad_niv_4,
	C.des_cobrabilidad_niv_5 ds_ren_cobrabilidad_niv_5,
	C.des_cobrabilidad_niv_6 ds_ren_cobrabilidad_niv_6,
	C.des_cobrabilidad_niv_7 ds_ren_cobrabilidad_niv_7,
	C.des_cobrabilidad_niv_8 ds_ren_cobrabilidad_niv_8,
	C.des_cobrabilidad_niv_9 ds_ren_cobrabilidad_niv_9,
	C.des_cobrabilidad_niv_10 ds_ren_cobrabilidad_niv_10,
	C.des_cobrabilidad_niv_11 ds_ren_cobrabilidad_niv_11,
	C.des_cobrabilidad_niv_12 ds_ren_cobrabilidad_niv_12,
	C.des_cobrabilidad_niv_13 ds_ren_cobrabilidad_niv_13,
	C.des_cobrabilidad_niv_14 ds_ren_cobrabilidad_niv_14,
	C.partition_date partition_date
FROM
	bi_corp_staging.rio157_ms0_dm_dwh_cobrabilidad_ C
WHERE
	C.partition_date in (SELECT max(partition_date) FROM bi_corp_staging.rio157_ms0_dm_dwh_cobrabilidad_);
"