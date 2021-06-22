set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set mapred.job.queue.name=root.dataeng;

INSERT
	OVERWRITE TABLE bi_corp_common.bt_ren_blce_result 
	PARTITION(partition_date)
SELECT
	RE.idf_cto_ods AS cod_ren_contrato,
	CASE
		WHEN LOWER(gen.fec_alta_cto) = 'null' THEN NULL
		ELSE from_unixtime(unix_timestamp(gen.fec_alta_cto , 'yyyyMMdd'), 'yyyy-MM-dd')
	END AS dt_ren_fec_alta_cto,
	CASE
		WHEN LOWER(gen.fec_ven) = 'null' THEN NULL
		ELSE from_unixtime(unix_timestamp(gen.fec_ven, 'yyyyMMdd'), 'yyyy-MM-dd')
	END AS dt_ren_fec_vto_cto,
	CASE
		WHEN LOWER(gen.fec_reestruc) = 'null' THEN NULL
		ELSE from_unixtime(unix_timestamp(gen.fec_reestruc, 'yyyyMMdd'), 'yyyy-MM-dd')
	END AS dt_ren_fec_reestruc_cto,
	CASE
		WHEN LOWER(gen.cod_producto) = 'null' THEN NULL
		ELSE trim(gen.cod_producto)
	END AS cod_ren_producto_generic,
	CASE
		WHEN LOWER(gen.cod_subprodu) = 'null' THEN NULL
		ELSE trim(gen.cod_subprodu)
	END AS cod_ren_subproducto_generic,
	TRIM(RE.cod_contenido) AS cod_ren_contenido,
	CASE
		WHEN LOWER(RE.fec_data) = 'null' THEN NULL
		ELSE from_unixtime(unix_timestamp(RE.fec_data, 'yyyyMMdd'), 'yyyy-MM-dd')
	END AS dt_ren_fec_data,
	TRIM(RE.cod_pais) AS cod_ren_pais,
	CAST(SUBSTR(RE.IDF_PERS_ODS, 6, 8) AS INT) AS cod_ren_nup,
	cli.cod_vincula AS cod_ren_vincula,
	cli.cod_tip_persona AS cod_ren_tipo_persona,
	concat(cli.nom_nombre, cli.nom_apellido_1) AS ds_ren_nombre_cliente,
	TRIM(RE.cod_entidad_espana) AS cod_ren_entidad_espana,
	TRIM(RE.cod_producto_gest) AS cod_ren_producto_gest,
	TRIM(RE.cod_divisa) AS cod_ren_divisa,
	TRIM(RE.cod_reajuste) AS cod_ren_reajuste,
	TRIM(RE.cod_agrp_func) AS cod_ren_agrp_func,
	TRIM(RE.cod_est_sdo) AS cod_ren_est_sdo,
	TRIM(RE.cod_cta_cont_gestion) AS cod_ren_cont_gestion,
	TRIM(RE.cod_area_negocio) AS cod_ren_area_negocio,
	CASE
		WHEN ADN.cod_area_negocio_niv_9 = 'null' THEN NULL
		ELSE trim(ADN.cod_area_negocio_niv_9)
	END AS cod_ren_division,
	CASE
		WHEN ADN.des_area_negocio_niv_9 = 'null' THEN NULL
		ELSE trim(ADN.des_area_negocio_niv_9)
	END AS ds_ren_division,
	TRIM(RE.cod_tip_ajuste) AS cod_ren_tipo_ajuste,
	TRIM(RE.cod_centro_cont) AS cod_ren_centro_cont,
	TRIM(RE.cod_ofi_comercial) AS cod_ren_ofi_comercial,
	TRIM(RE.ind_conciliacion) AS cod_ren_conciliacion,
	CASE
		WHEN TRIM(RE.ind_pool)= 'S' THEN 1
		ELSE 0
	END AS flag_ren_ind_pool,
	TRIM(RE.cod_segmento_gest) AS cod_ren_segmento_gest,
	TRIM(RE.cod_gestor) AS cod_ren_gestor,
	TRIM(RE.cod_univ) AS cod_ren_univ,
	TRIM(RE.cod_gestor_prod) AS cod_ren_gestor_prod,
	TRIM(RE.cod_origen_inf) AS cod_ren_origen_inf,
	TRIM(entj.cod_hijo) AS cod_ren_entidad_hijo,
	CASE
		WHEN LOWER(prod.cod_producto_niv_1) = 'null' THEN NULL
		ELSE trim(prod.cod_producto_niv_1)
	END AS cod_ren_producto_niv_1,
	CASE
		WHEN LOWER(prod.cod_producto_niv_2) = 'null' THEN NULL
		ELSE trim(prod.cod_producto_niv_2)
	END AS cod_ren_producto_niv_2,
	CASE
		WHEN LOWER(prod.cod_producto_niv_3) = 'null' THEN NULL
		ELSE trim(prod.cod_producto_niv_3)
	END AS cod_ren_producto_niv_3,
	CASE
		WHEN LOWER(prod.cod_producto_niv_4) = 'null' THEN NULL
		ELSE trim(prod.cod_producto_niv_4)
	END AS cod_ren_producto_niv_4,
	CASE
		WHEN LOWER(prod.cod_producto_niv_5) = 'null' THEN NULL
		ELSE trim(prod.cod_producto_niv_5)
	END AS cod_ren_producto_niv_5,
	CASE
		WHEN LOWER(prod.cod_producto_niv_6) = 'null' THEN NULL
		ELSE trim(prod.cod_producto_niv_6)
	END AS cod_ren_producto_niv_6,
	CASE
		WHEN LOWER(prod.cod_producto_niv_7) = 'null' THEN NULL
		ELSE trim(prod.cod_producto_niv_7)
	END AS cod_ren_producto_niv_7,
	CASE
		WHEN LOWER(prod.cod_producto_niv_8) = 'null' THEN NULL
		ELSE trim(prod.cod_producto_niv_8)
	END AS cod_ren_producto_niv_8,
	CASE
		WHEN LOWER(prod.cod_producto_niv_9) = 'null' THEN NULL
		ELSE trim(prod.cod_producto_niv_9)
	END AS cod_ren_producto_niv_9,
	CASE
		WHEN LOWER(prod.cod_producto_niv_10) = 'null' THEN NULL
		ELSE trim(prod.cod_producto_niv_10)
	END AS cod_ren_producto_niv_10,
	CASE
		WHEN LOWER(prod.des_producto_niv_1) = 'null' THEN NULL
		ELSE trim(prod.des_producto_niv_1)
	END AS ds_ren_producto_niv_1,
	CASE
		WHEN LOWER(prod.des_producto_niv_2) = 'null' THEN NULL
		ELSE trim(prod.des_producto_niv_2)
	END AS ds_ren_producto_niv_2,
	CASE
		WHEN LOWER(prod.des_producto_niv_3) = 'null' THEN NULL
		ELSE trim(prod.des_producto_niv_3)
	END AS ds_ren_producto_niv_3,
	CASE
		WHEN LOWER(prod.des_producto_niv_4) = 'null' THEN NULL
		ELSE trim(prod.des_producto_niv_4)
	END AS ds_ren_producto_niv_4,
	CASE
		WHEN LOWER(prod.des_producto_niv_5) = 'null' THEN NULL
		ELSE trim(prod.des_producto_niv_5)
	END AS ds_ren_producto_niv_5,
	CASE
		WHEN LOWER(prod.des_producto_niv_6) = 'null' THEN NULL
		ELSE trim(prod.des_producto_niv_6)
	END AS ds_ren_producto_niv_6,
	CASE
		WHEN LOWER(prod.des_producto_niv_7) = 'null' THEN NULL
		ELSE trim(prod.des_producto_niv_7)
	END AS ds_ren_producto_niv_7,
	CASE
		WHEN LOWER(prod.des_producto_niv_8) = 'null' THEN NULL
		ELSE trim(prod.des_producto_niv_8)
	END AS ds_ren_producto_niv_8,
	CASE
		WHEN LOWER(prod.des_producto_niv_9) = 'null' THEN NULL
		ELSE trim(prod.des_producto_niv_9)
	END AS ds_ren_producto_niv_9,
	CASE
		WHEN LOWER(prod.des_producto_niv_10) = 'null' THEN NULL
		ELSE trim(prod.des_producto_niv_10)
	END AS ds_ren_producto_niv_10,
	CASE
		WHEN LOWER(cta.cod_cta_resultados_niv_1) = 'null' THEN NULL
		ELSE trim(cta.cod_cta_resultados_niv_1)
	END AS cod_ren_cta_resultados_niv_1,
	CASE
		WHEN LOWER(cta.cod_cta_resultados_niv_2) = 'null' THEN NULL
		ELSE trim(cta.cod_cta_resultados_niv_2)
	END AS cod_ren_cta_resultados_niv_2,
	CASE
		WHEN LOWER(cta.cod_cta_resultados_niv_3) = 'null' THEN NULL
		ELSE trim(cta.cod_cta_resultados_niv_3)
	END AS cod_ren_cta_resultados_niv_3,
	CASE
		WHEN LOWER(cta.cod_cta_resultados_niv_4) = 'null' THEN NULL
		ELSE trim(cta.cod_cta_resultados_niv_4)
	END AS cod_ren_cta_resultados_niv_4,
	CASE
		WHEN LOWER(cta.cod_cta_resultados_niv_5) = 'null' THEN NULL
		ELSE trim(cta.cod_cta_resultados_niv_5)
	END AS cod_ren_cta_resultados_niv_5,
	CASE
		WHEN LOWER(cta.cod_cta_resultados_niv_6) = 'null' THEN NULL
		ELSE trim(cta.cod_cta_resultados_niv_6)
	END AS cod_ren_cta_resultados_niv_6,
	CASE
		WHEN LOWER(cta.cod_cta_resultados_niv_7) = 'null' THEN NULL
		ELSE trim(cta.cod_cta_resultados_niv_7)
	END AS cod_ren_cta_resultados_niv_7,
	CASE
		WHEN LOWER(cta.cod_cta_resultados_niv_8) = 'null' THEN NULL
		ELSE trim(cta.cod_cta_resultados_niv_8)
	END AS cod_ren_cta_resultados_niv_8,
	CASE
		WHEN LOWER(cta.cod_cta_resultados_niv_9) = 'null' THEN NULL
		ELSE trim(cta.cod_cta_resultados_niv_9)
	END AS cod_ren_cta_resultados_niv_9,
	CASE
		WHEN LOWER(cta.cod_cta_resultados_niv_10) = 'null' THEN NULL
		ELSE trim(cta.cod_cta_resultados_niv_10)
	END AS cod_ren_cta_resultados_niv_10,
	CASE
		WHEN LOWER(cta.cod_cta_resultados_niv_11) = 'null' THEN NULL
		ELSE trim(cta.cod_cta_resultados_niv_11)
	END AS cod_ren_cta_resultados_niv_11,
	CASE
		WHEN LOWER(cta.cod_cta_resultados_niv_12) = 'null' THEN NULL
		ELSE trim(cta.cod_cta_resultados_niv_12)
	END AS cod_ren_cta_resultados_niv_12,
	CASE
		WHEN LOWER(cta.des_cta_resultados_niv_1) = 'null' THEN NULL
		ELSE trim(cta.des_cta_resultados_niv_1)
	END AS ds_ren_cta_resultados_niv_1,
	CASE
		WHEN LOWER(cta.des_cta_resultados_niv_2) = 'null' THEN NULL
		ELSE trim(cta.des_cta_resultados_niv_2)
	END AS ds_ren_cta_resultados_niv_2,
	CASE
		WHEN LOWER(cta.des_cta_resultados_niv_3) = 'null' THEN NULL
		ELSE trim(cta.des_cta_resultados_niv_3)
	END AS ds_ren_cta_resultados_niv_3,
	CASE
		WHEN LOWER(cta.des_cta_resultados_niv_4) = 'null' THEN NULL
		ELSE trim(cta.des_cta_resultados_niv_4)
	END AS ds_ren_cta_resultados_niv_4,
	CASE
		WHEN LOWER(cta.des_cta_resultados_niv_5) = 'null' THEN NULL
		ELSE trim(cta.des_cta_resultados_niv_5)
	END AS ds_ren_cta_resultados_niv_5,
	CASE
		WHEN LOWER(cta.des_cta_resultados_niv_6) = 'null' THEN NULL
		ELSE trim(cta.des_cta_resultados_niv_6)
	END AS ds_ren_cta_resultados_niv_6,
	CASE
		WHEN LOWER(cta.des_cta_resultados_niv_7) = 'null' THEN NULL
		ELSE trim(cta.des_cta_resultados_niv_7)
	END AS ds_ren_cta_resultados_niv_7,
	CASE
		WHEN LOWER(cta.des_cta_resultados_niv_8) = 'null' THEN NULL
		ELSE trim(cta.des_cta_resultados_niv_8)
	END AS ds_ren_cta_resultados_niv_8,
	CASE
		WHEN LOWER(cta.des_cta_resultados_niv_9) = 'null' THEN NULL
		ELSE trim(cta.des_cta_resultados_niv_9)
	END AS ds_ren_cta_resultados_niv_9,
	CASE
		WHEN LOWER(cta.des_cta_resultados_niv_10) = 'null' THEN NULL
		ELSE trim(cta.des_cta_resultados_niv_10)
	END AS ds_ren_cta_resultados_niv_10,
	CASE
		WHEN LOWER(cta.des_cta_resultados_niv_11) = 'null' THEN NULL
		ELSE trim(cta.des_cta_resultados_niv_11)
	END AS ds_ren_cta_resultados_niv_11,
	CASE
		WHEN LOWER(cta.des_cta_resultados_niv_12) = 'null' THEN NULL
		ELSE trim(cta.des_cta_resultados_niv_12)
	END AS ds_ren_cta_resultados_niv_12,
	CASE
		WHEN LOWER(ADN.cod_area_negocio_niv_1) = 'null' THEN NULL
		ELSE trim(ADN.cod_area_negocio_niv_1)
	END AS cod_ren_area_negocio_niv_1,
	CASE
		WHEN LOWER(ADN.cod_area_negocio_niv_2) = 'null' THEN NULL
		ELSE trim(ADN.cod_area_negocio_niv_2)
	END AS cod_ren_area_negocio_niv_2,
	CASE
		WHEN LOWER(ADN.cod_area_negocio_niv_3) = 'null' THEN NULL
		ELSE trim(ADN.cod_area_negocio_niv_3)
	END AS cod_ren_area_negocio_niv_3,
	CASE
		WHEN LOWER(ADN.cod_area_negocio_niv_4) = 'null' THEN NULL
		ELSE trim(ADN.cod_area_negocio_niv_4)
	END AS cod_ren_area_negocio_niv_4,
	CASE
		WHEN LOWER(ADN.cod_area_negocio_niv_5) = 'null' THEN NULL
		ELSE trim(ADN.cod_area_negocio_niv_5)
	END AS cod_ren_area_negocio_niv_5,
	CASE
		WHEN LOWER(ADN.cod_area_negocio_niv_6) = 'null' THEN NULL
		ELSE trim(ADN.cod_area_negocio_niv_6)
	END AS cod_ren_area_negocio_niv_6,
	CASE
		WHEN LOWER(ADN.cod_area_negocio_niv_7) = 'null' THEN NULL
		ELSE trim(ADN.cod_area_negocio_niv_7)
	END AS cod_ren_area_negocio_niv_7,
	CASE
		WHEN LOWER(ADN.des_area_negocio_niv_1) = 'null' THEN NULL
		ELSE trim(ADN.des_area_negocio_niv_1)
	END AS ds_ren_area_negocio_niv_1,
	CASE
		WHEN LOWER(ADN.des_area_negocio_niv_2) = 'null' THEN NULL
		ELSE trim(ADN.des_area_negocio_niv_2)
	END AS ds_ren_area_negocio_niv_2,
	CASE
		WHEN LOWER(ADN.des_area_negocio_niv_3) = 'null' THEN NULL
		ELSE trim(ADN.des_area_negocio_niv_3)
	END AS ds_ren_area_negocio_niv_3,
	CASE
		WHEN LOWER(ADN.des_area_negocio_niv_4) = 'null' THEN NULL
		ELSE trim(ADN.des_area_negocio_niv_4)
	END AS ds_ren_area_negocio_niv_4,
	CASE
		WHEN LOWER(ADN.des_area_negocio_niv_5) = 'null' THEN NULL
		ELSE trim(ADN.des_area_negocio_niv_5)
	END AS ds_ren_area_negocio_niv_5,
	CASE
		WHEN LOWER(ADN.des_area_negocio_niv_6) = 'null' THEN NULL
		ELSE trim(ADN.des_area_negocio_niv_6)
	END AS ds_ren_area_negocio_niv_6,
	CASE
		WHEN LOWER(ADN.des_area_negocio_niv_7) = 'null' THEN NULL
		ELSE trim(ADN.des_area_negocio_niv_7)
	END AS ds_ren_area_negocio_niv_7,
	CASE
		WHEN LOWER(ENT.cod_entidad_gest_niv_1) = 'null' THEN NULL
		ELSE trim(ENT.cod_entidad_gest_niv_1)
	END AS cod_ren_entidad_gest_niv_1,
	CASE
		WHEN LOWER(ENT.cod_entidad_gest_niv_2) = 'null' THEN NULL
		ELSE trim(ENT.cod_entidad_gest_niv_2)
	END AS cod_ren_entidad_gest_niv_2,
	CASE
		WHEN LOWER(ENT.cod_entidad_gest_niv_3) = 'null' THEN NULL
		ELSE trim(ENT.cod_entidad_gest_niv_3)
	END AS cod_ren_entidad_gest_niv_3,
	CASE
		WHEN LOWER(ENT.cod_entidad_gest_niv_4) = 'null' THEN NULL
		ELSE trim(ENT.cod_entidad_gest_niv_4)
	END AS cod_ren_entidad_gest_niv_4,
	CASE
		WHEN LOWER(ENT.cod_entidad_gest_niv_5) = 'null' THEN NULL
		ELSE trim(ENT.cod_entidad_gest_niv_5)
	END AS cod_ren_entidad_gest_niv_5,
	CASE
		WHEN LOWER(ENT.cod_entidad_gest_niv_6) = 'null' THEN NULL
		ELSE trim(ENT.cod_entidad_gest_niv_6)
	END AS cod_ren_entidad_gest_niv_6,
	CASE
		WHEN LOWER(ENT.des_entidad_gest_niv_1) = 'null' THEN NULL
		ELSE trim(ENT.des_entidad_gest_niv_1)
	END AS ds_ren_entidad_gest_niv_1,
	CASE
		WHEN LOWER(ENT.des_entidad_gest_niv_2) = 'null' THEN NULL
		ELSE trim(ENT.des_entidad_gest_niv_2)
	END AS ds_ren_entidad_gest_niv_2,
	CASE
		WHEN LOWER(ENT.des_entidad_gest_niv_3) = 'null' THEN NULL
		ELSE trim(ENT.des_entidad_gest_niv_3)
	END AS ds_ren_entidad_gest_niv_3,
	CASE
		WHEN LOWER(ENT.des_entidad_gest_niv_4) = 'null' THEN NULL
		ELSE trim(ENT.des_entidad_gest_niv_4)
	END AS ds_ren_entidad_gest_niv_4,
	CASE
		WHEN LOWER(ENT.des_entidad_gest_niv_5) = 'null' THEN NULL
		ELSE trim(ENT.des_entidad_gest_niv_5)
	END AS ds_ren_entidad_gest_niv_5,
	CASE
		WHEN LOWER(ENT.des_entidad_gest_niv_6) = 'null' THEN NULL
		ELSE trim(ENT.des_entidad_gest_niv_6)
	END AS ds_ren_entidad_gest_niv_6,
	CASE
		WHEN LOWER(OFFI.cod_ofi_comercial_niv_5) = 'null' THEN NULL
		ELSE trim(OFFI.cod_ofi_comercial_niv_5)
	END AS cod_ren_ofi_comercial_niv_5,
	CASE
		WHEN LOWER(OFFI.cod_ofi_comercial_niv_6) = 'null' THEN NULL
		ELSE trim(OFFI.cod_ofi_comercial_niv_6)
	END AS cod_ren_ofi_comercial_niv_6,
	CASE
		WHEN LOWER(OFFI.cod_ofi_comercial_niv_7) = 'null' THEN NULL
		ELSE trim(OFFI.cod_ofi_comercial_niv_7)
	END AS cod_ren_ofi_comercial_niv_7,
	CASE
		WHEN LOWER(OFFI.cod_ofi_comercial_niv_8) = 'null' THEN NULL
		ELSE trim(OFFI.cod_ofi_comercial_niv_8)
	END AS cod_ren_ofi_comercial_niv_8,
	CASE
		WHEN LOWER(OFFI.cod_ofi_comercial_niv_9) = 'null' THEN NULL
		ELSE trim(OFFI.cod_ofi_comercial_niv_9)
	END AS cod_ren_ofi_comercial_niv_9,
	CASE
		WHEN LOWER(OFFI.cod_ofi_comercial_niv_10) = 'null' THEN NULL
		ELSE trim(OFFI.cod_ofi_comercial_niv_10)
	END AS cod_ren_ofi_comercial_niv_10,
	CASE
		WHEN LOWER(OFFI.cod_ofi_comercial_niv_11) = 'null' THEN NULL
		ELSE trim(OFFI.cod_ofi_comercial_niv_11)
	END AS cod_ren_ofi_comercial_niv_11,
	CASE
		WHEN LOWER(OFFI.des_ofi_comercial_niv_5) = 'null' THEN NULL
		ELSE trim(OFFI.des_ofi_comercial_niv_5)
	END AS ds_ren_ofi_comercial_niv_5,
	CASE
		WHEN LOWER(OFFI.des_ofi_comercial_niv_6) = 'null' THEN NULL
		ELSE trim(OFFI.des_ofi_comercial_niv_6)
	END AS ds_ren_ofi_comercial_niv_6,
	CASE
		WHEN LOWER(OFFI.des_ofi_comercial_niv_7) = 'null' THEN NULL
		ELSE trim(OFFI.des_ofi_comercial_niv_7)
	END AS ds_ren_ofi_comercial_niv_7,
	CASE
		WHEN LOWER(OFFI.des_ofi_comercial_niv_8) = 'null' THEN NULL
		ELSE trim(OFFI.des_ofi_comercial_niv_8)
	END AS ds_ren_ofi_comercial_niv_8,
	CASE
		WHEN LOWER(OFFI.des_ofi_comercial_niv_9) = 'null' THEN NULL
		ELSE trim(OFFI.des_ofi_comercial_niv_9)
	END AS ds_ren_ofi_comercial_niv_9,
	CASE
		WHEN LOWER(OFFI.des_ofi_comercial_niv_10) = 'null' THEN NULL
		ELSE trim(OFFI.des_ofi_comercial_niv_10)
	END AS ds_ren_ofi_comercial_niv_10,
	CASE
		WHEN LOWER(OFFI.des_ofi_comercial_niv_11) = 'null' THEN NULL
		ELSE trim(OFFI.des_ofi_comercial_niv_11)
	END AS ds_ren_ofi_comercial_niv_11,
	CASE
		WHEN LOWER(GEST.cod_gestor_niv_1) = 'null' THEN NULL
		ELSE trim(GEST.cod_gestor_niv_1)
	END AS cod_ren_gestor_niv_1,
	CASE
		WHEN LOWER(GEST.cod_gestor_niv_2) = 'null' THEN NULL
		ELSE trim(GEST.cod_gestor_niv_2)
	END AS cod_ren_gestor_niv_2,
	CASE
		WHEN LOWER(GEST.cod_gestor_niv_3) = 'null' THEN NULL
		ELSE trim(GEST.cod_gestor_niv_3)
	END AS cod_ren_gestor_niv_3,
	CASE
		WHEN LOWER(GEST.cod_gestor_niv_4) = 'null' THEN NULL
		ELSE trim(GEST.cod_gestor_niv_4)
	END AS cod_ren_gestor_niv_4,
	CASE
		WHEN LOWER(GEST.cod_gestor_niv_5) = 'null' THEN NULL
		ELSE trim(GEST.cod_gestor_niv_5)
	END AS cod_ren_gestor_niv_5,
	CASE
		WHEN LOWER(GEST.cod_gestor_niv_6) = 'null' THEN NULL
		ELSE trim(GEST.cod_gestor_niv_6)
	END AS cod_ren_gestor_niv_6,
	CASE
		WHEN LOWER(GEST.cod_gestor_niv_7) = 'null' THEN NULL
		ELSE trim(GEST.cod_gestor_niv_7)
	END AS cod_ren_gestor_niv_7,
	CASE
		WHEN LOWER(GEST.cod_gestor_niv_8) = 'null' THEN NULL
		ELSE trim(GEST.cod_gestor_niv_8)
	END AS cod_ren_gestor_niv_8,
	CASE
		WHEN LOWER(GEST.cod_gestor_niv_9) = 'null' THEN NULL
		ELSE trim(GEST.cod_gestor_niv_9)
	END AS cod_ren_gestor_niv_9,
	CASE
		WHEN LOWER(GEST.cod_gestor_niv_10) = 'null' THEN NULL
		ELSE trim(GEST.cod_gestor_niv_10)
	END AS cod_ren_gestor_niv_10,
	CASE
		WHEN LOWER(GEST.cod_gestor_niv_11) = 'null' THEN NULL
		ELSE trim(GEST.cod_gestor_niv_11)
	END AS cod_ren_gestor_niv_11,
	CASE
		WHEN LOWER(GEST.des_gestor_niv_1) = 'null' THEN NULL
		ELSE trim(GEST.des_gestor_niv_1)
	END AS ds_ren_gestor_niv_1,
	CASE
		WHEN LOWER(GEST.des_gestor_niv_2) = 'null' THEN NULL
		ELSE trim(GEST.des_gestor_niv_2)
	END AS ds_ren_gestor_niv_2,
	CASE
		WHEN LOWER(GEST.des_gestor_niv_3) = 'null' THEN NULL
		ELSE trim(GEST.des_gestor_niv_3)
	END AS ds_ren_gestor_niv_3,
	CASE
		WHEN LOWER(GEST.des_gestor_niv_4) = 'null' THEN NULL
		ELSE trim(GEST.des_gestor_niv_4)
	END AS ds_ren_gestor_niv_4,
	CASE
		WHEN LOWER(GEST.des_gestor_niv_5) = 'null' THEN NULL
		ELSE trim(GEST.des_gestor_niv_5)
	END AS ds_ren_gestor_niv_5,
	CASE
		WHEN LOWER(GEST.des_gestor_niv_6) = 'null' THEN NULL
		ELSE trim(GEST.des_gestor_niv_6)
	END AS ds_ren_gestor_niv_6,
	CASE
		WHEN LOWER(GEST.des_gestor_niv_7) = 'null' THEN NULL
		ELSE trim(GEST.des_gestor_niv_7)
	END AS ds_ren_gestor_niv_7,
	CASE
		WHEN LOWER(GEST.des_gestor_niv_8) = 'null' THEN NULL
		ELSE trim(GEST.des_gestor_niv_8)
	END AS ds_ren_gestor_niv_8,
	CASE
		WHEN LOWER(GEST.des_gestor_niv_9) = 'null' THEN NULL
		ELSE trim(GEST.des_gestor_niv_9)
	END AS ds_ren_gestor_niv_9,
	CASE
		WHEN LOWER(GEST.des_gestor_niv_10) = 'null' THEN NULL
		ELSE trim(GEST.des_gestor_niv_10)
	END AS ds_ren_gestor_niv_10,
	CASE
		WHEN LOWER(GEST.des_gestor_niv_11) = 'null' THEN NULL
		ELSE trim(GEST.des_gestor_niv_11)
	END AS ds_ren_gestor_niv_11,
	sum(COALESCE(RE.OUT_TTI, 0)) AS fc_ren_resultado_saldo_ficticio,
	sum(COALESCE(RE.IMP_ING_PER_ML, 0)) AS fc_ren_imp_ing_per_ml,
	sum(COALESCE(RE.IMP_ING_CAP_ML, 0)) AS fc_ren_imp_ing_cap_ml,
	sum(COALESCE(RE.IMP_ING_REAJ_ML, 0)) AS fc_ren_ing_reaj_ml,
	sum(COALESCE(RE.IMP_EGR_PER_ML, 0)) AS fc_ren_egr_per_ml,
	sum(COALESCE(RE.IMP_EGR_CAP_ML, 0)) AS fc_ren_egr_cap_ml,
	sum(COALESCE(RE.IMP_EGR_REAJ_ML, 0)) AS fc_ren_egr_reaj_ml,
	sum(COALESCE(RE.IMP_AJTTI_EGR_TB_CAP_ML, 0)) AS fc_ren_ajtti_egr_tb_cap_ml,
	sum(COALESCE(RE.IMP_AJTTI_EGR_SL_CAP_ML, 0)) AS fc_ren_ajtti_egr_sl_cap_ml,
	sum(COALESCE(RE.IMP_AJTTI_EGR_PER_ML, 0)) AS fc_ren_ajtti_egr_per_ml,
	sum(COALESCE(RE.IMP_AJTTI_EGR_REAJUSTE_ML, 0)) AS fc_ren_ajtti_egr_reajuste_ml,
	sum(COALESCE(RE.IMP_AJTTI_ING_TB_CAP_ML, 0)) AS fc_ren_ajtti_ing_tb_cap_ml,
	sum(COALESCE(RE.IMP_AJTTI_ING_SL_CAP_ML, 0)) AS fc_ren_ajtti_ing_sl_cap_ml,
	sum(COALESCE(RE.IMP_AJTTI_ING_PER_ML, 0)) AS fc_ren_ajtti_ing_per_ml,
	sum(COALESCE(RE.IMP_AJTTI_ING_REAJUSTE_ML, 0)) AS fc_ren_ajtti_ing_reajuste_ml,
	sum(COALESCE(RE.IMP_EFEC_ENC_ML, 0)) AS fc_ren_efec_enc_ml,
	sum(COALESCE(RE.IMP_ING_PER_MO, 0)) AS fc_ren_ing_per_mo,
	sum(COALESCE(RE.IMP_ING_CAP_MO, 0)) AS fc_ren_ing_cap_mo,
	sum(COALESCE(RE.IMP_ING_REAJ_MO, 0)) AS fc_ren_ing_reaj_mo,
	sum(COALESCE(RE.IMP_EGR_PER_MO, 0)) AS fc_ren_egr_per_mo,
	sum(COALESCE(RE.IMP_EGR_CAP_MO, 0)) AS fc_ren_egr_cap_mo,
	sum(COALESCE(RE.IMP_EGR_REAJ_MO, 0)) AS fc_ren_egr_reaj_mo,
	sum(COALESCE(RE.IMP_AJTTI_EGR_TB_CAP_MO, 0)) AS fc_ren_ajtti_egr_tb_cap_mo,
	sum(COALESCE(RE.IMP_AJTTI_EGR_SL_CAP_MO, 0)) AS fc_ren_ajtti_egr_sl_cap_mo,
	sum(COALESCE(RE.IMP_AJTTI_EGR_PER_MO, 0)) AS fc_ren_ajtti_egr_per_mo,
	sum(COALESCE(RE.IMP_AJTTI_EGR_REAJUSTE_MO, 0)) AS fc_ren_ajtti_egr_reajuste_mo,
	sum(COALESCE(RE.IMP_AJTTI_ING_TB_CAP_MO, 0)) AS fc_ren_ajtti_ing_tb_cap_mo,
	sum(COALESCE(RE.IMP_AJTTI_ING_SL_CAP_MO, 0)) AS fc_ren_ajtti_ing_sl_cap_mo,
	sum(COALESCE(RE.IMP_AJTTI_ING_PER_MO, 0)) AS fc_ren_ajtti_ing_per_mo,
	sum(COALESCE(RE.IMP_AJTTI_ING_REAJUSTE_MO, 0)) AS fc_ren_ajtti_ing_reajuste_mo,
	sum(COALESCE(RE.IMP_EFEC_ENC_MO, 0)) AS fc_ren_efec_enc_mo,
	sum(COALESCE(RE.IMP_SDO_CAP_MED_ML, 0)) AS fc_ren_sdo_cap_med_ml,
	sum(COALESCE(RE.IMP_SDO_MED_INT_ML, 0)) AS fc_ren_sdo_med_int_ml,
	sum(COALESCE(RE.IMP_SDO_MED_REAJUSTE_ML, 0)) AS fc_ren_sdo_med_reajuste_ml,
	sum(COALESCE(RE.IMP_SDO_MED_INSOLV_ML, 0)) AS fc_ren_sdo_med_insolv_ml,
	sum(COALESCE(RE.IMP_SDO_CAP_MED_MO, 0)) AS fc_ren_sdo_cap_med_mo,
	sum(COALESCE(RE.IMP_SDO_MED_INT_MO, 0)) AS fc_ren_sdo_med_int_mo,
	sum(COALESCE(RE.IMP_SDO_MED_REAJUSTE_MO, 0)) AS fc_ren_sdo_med_reajuste_mo,
	sum(COALESCE(RE.IMP_SDO_MED_INSOLV_MO, 0)) AS fc_ren_sdo_med_insolv_mo,
	sum(COALESCE(RE.IMP_ING_PER_ML, 0)) AS fc_ren_ing_per_ml,
	(sum(COALESCE(RE.IMP_ING_PER_ML, 0)) + sum(COALESCE(RE.IMP_ING_CAP_ML, 0)) + sum(COALESCE(RE.IMP_ING_REAJ_ML, 0)) + sum(COALESCE(RE.IMP_EGR_PER_ML, 0)) + sum(COALESCE(RE.IMP_EGR_CAP_ML, 0)) + sum(COALESCE(RE.IMP_EGR_REAJ_ML, 0)) + sum(COALESCE(RE.IMP_AJTTI_EGR_TB_CAP_ML, 0)) + sum(COALESCE(RE.IMP_AJTTI_EGR_SL_CAP_ML, 0)) + sum(COALESCE(RE.IMP_AJTTI_EGR_PER_ML, 0)) + sum(COALESCE(RE.IMP_AJTTI_EGR_REAJUSTE_ML, 0)) + sum(COALESCE(RE.IMP_AJTTI_ING_TB_CAP_ML, 0)) + sum(COALESCE(RE.IMP_AJTTI_ING_SL_CAP_ML, 0)) + sum(COALESCE(RE.IMP_AJTTI_ING_PER_ML, 0)) + sum(COALESCE(RE.IMP_AJTTI_ING_REAJUSTE_ML, 0)) + sum(COALESCE(RE.IMP_EFEC_ENC_ML, 0))) AS fc_ren_restultado_total_ML,
	(sum(COALESCE(RE.IMP_ING_PER_MO, 0)) + sum(COALESCE(RE.IMP_ING_CAP_MO, 0)) + sum(COALESCE(RE.IMP_ING_REAJ_MO, 0)) + sum(COALESCE(RE.IMP_EGR_PER_MO, 0)) + sum(COALESCE(RE.IMP_EGR_CAP_ML, 0)) + sum(COALESCE(RE.IMP_EGR_REAJ_MO, 0)) + sum(COALESCE(RE.IMP_AJTTI_EGR_TB_CAP_MO, 0)) + sum(COALESCE(RE.IMP_AJTTI_EGR_SL_CAP_MO, 0)) + sum(COALESCE(RE.IMP_AJTTI_EGR_PER_MO, 0)) + sum(COALESCE(RE.IMP_AJTTI_EGR_REAJUSTE_MO, 0)) + sum(COALESCE(RE.IMP_AJTTI_ING_TB_CAP_MO, 0)) + sum(COALESCE(RE.IMP_AJTTI_ING_SL_CAP_MO, 0)) + sum(COALESCE(RE.IMP_AJTTI_ING_PER_MO, 0)) + sum(COALESCE(RE.IMP_AJTTI_ING_REAJUSTE_MO, 0)) + sum(COALESCE(RE.IMP_EFEC_ENC_MO, 0))) AS fc_ren_restultado_total_MO,
	(sum(COALESCE(RE.IMP_SDO_CAP_MED_ML, 0)) + sum(COALESCE(RE.IMP_SDO_MED_INT_ML, 0)) + sum(COALESCE(RE.IMP_SDO_MED_REAJUSTE_ML, 0)) + sum(COALESCE(RE.IMP_SDO_MED_INSOLV_ML, 0))) AS fc_ren_saldo_medio_total_ML,
	(sum(COALESCE(RE.IMP_SDO_CAP_MED_MO, 0)) + sum(COALESCE(RE.IMP_SDO_MED_INT_MO, 0)) + sum(COALESCE(RE.IMP_SDO_MED_REAJUSTE_MO, 0)) + sum(COALESCE(RE.IMP_SDO_MED_INSOLV_MO, 0))) AS fc_ren_saldo_medio_total_MO,
	(sum(COALESCE(RE.IMP_ING_PER_ML, 0)) + sum(COALESCE(RE.IMP_ING_CAP_ML, 0)) + sum(COALESCE(RE.IMP_ING_REAJ_ML, 0)) + sum(COALESCE(RE.IMP_EGR_PER_ML, 0)) + sum(COALESCE(RE.IMP_EGR_CAP_ML, 0)) + sum(COALESCE(RE.IMP_EGR_REAJ_ML, 0))) AS fc_ren_resultado_total_real_ML,
	(sum(COALESCE(RE.IMP_ING_PER_MO, 0)) + sum(COALESCE(RE.IMP_ING_CAP_MO, 0)) + sum(COALESCE(RE.IMP_ING_REAJ_MO, 0)) + sum(COALESCE(RE.IMP_EGR_PER_MO, 0)) + sum(COALESCE(RE.IMP_EGR_CAP_MO, 0)) + sum(COALESCE(RE.IMP_EGR_REAJ_MO, 0))) AS fc_ren_resultado_total_real_MO,
	(sum(COALESCE(RE.IMP_SDO_CAP_ML, 0)) + sum(COALESCE(RE.IMP_SDO_INT_ML, 0)) + sum(COALESCE(RE.IMP_SDO_INSOLV_ML, 0)) + sum(COALESCE(RE.IMP_SDO_REAJUSTE_ML, 0))) AS fc_ren_saldo_puntual_ML,
	(sum(COALESCE(RE.IMP_SDO_CAP_MO, 0)) + sum(COALESCE(RE.IMP_SDO_INT_MO, 0)) + sum(COALESCE(RE.IMP_SDO_INSOLV_MO, 0)) + sum(COALESCE(RE.IMP_SDO_REAJUSTE_MO, 0))) AS fc_ren_saldo_puntual_MO--,
	--RE.partition_date
FROM
	bi_corp_staging.rio157_ms0_ft_dwh_blce_result RE
LEFT JOIN bi_corp_staging.rio157_ms0_dm_dwh_productos_ctr prod ON
	RE.cod_producto_gest = prod.cod_producto
	AND prod.partition_date = '2020-07-14'
	AND RE.partition_date = '2020-05-31'
	AND UPPER(prod.cod_jerq_pr01) = 'JBLDN'
LEFT JOIN bi_corp_staging.rio157_ms0_dm_dwh_area_negocio_ctr adn ON
	RE.cod_area_negocio = adn.cod_area_negocio
	AND adn.partition_date = '2020-07-14'
	AND UPPER(adn.cod_jerq_adn01) = 'JAN01'
	AND RE.cod_pais = adn.cod_pais
LEFT JOIN bi_corp_staging.rio157_ms0_dm_dwh_cta_resultados_ctr cta ON
	RE.cod_cta_cont_gestion = cta.cod_cta_resultados
	AND cta.partition_date = '2020-07-10'
	AND UPPER(cta.COD_JERQ_CR01) = 'JCNBV'
	AND RE.cod_pais = cta.cod_pais
LEFT JOIN bi_corp_staging.rio157_ms0_dm_dwh_entidad_juridica_ ent ON
	RE.cod_entidad_espana = ent.cod_entidad_gest
	AND ent.partition_date = '2020-05-30'
	AND UPPER(ent.cod_jerq_ej01) = 'JEJ01'
	AND RE.cod_pais = ent.cod_pais
LEFT JOIN bi_corp_staging.rio157_ms0_dm_dwh_oficina offi ON
	RE.cod_ofi_comercial = offi.cod_ofi_comercial
	AND offi.partition_date = '2020-05-30'
	AND UPPER(offi.cod_jerq_oc01) = 'JOC01'
	AND RE.cod_pais = offi.cod_pais
LEFT JOIN bi_corp_staging.rio157_ms0_dm_dwh_gestor_ gest ON
	RE.cod_gestor = gest.cod_gestor
	AND gest.partition_date = '2020-05-30'
	AND RE.cod_pais = gest.cod_pais
LEFT JOIN bi_corp_staging.rio157_ms0_dt_dwh_cliente_result cli ON
	RE.idf_pers_ods = cli.idf_pers_ods
	AND cli.partition_date = '2020-05-31'
	AND RE.cod_pais = cli.cod_pais
LEFT JOIN bi_corp_staging.rio157_ms0_dt_dwh_generic_result gen ON
	RE.idf_cto_ods = gen.idf_cto_ods
	AND gen.partition_date = '2020-05-31'
	AND gen.cod_contenido = RE.cod_contenido
	AND RE.cod_pais = gen.cod_pais
LEFT JOIN bi_corp_staging.rio157_ms0_dm_je_dwh_entidades_ctr entj ON
	RE.cod_pais = entj.cod_pais
	AND entj.partition_date = '2020-07-14'
WHERE
	RE.partition_date = '2020-05-31'
GROUP BY
	RE.idf_cto_ods ,
	CASE
		WHEN LOWER(gen.fec_alta_cto) = 'null' THEN NULL
		ELSE from_unixtime(unix_timestamp(gen.fec_alta_cto , 'yyyyMMdd'), 'yyyy-MM-dd')
	END,
	CASE
		WHEN LOWER(gen.fec_ven) = 'null' THEN NULL
		ELSE from_unixtime(unix_timestamp(gen.fec_ven, 'yyyyMMdd'), 'yyyy-MM-dd')
	END,
	CASE
		WHEN LOWER(gen.fec_reestruc) = 'null' THEN NULL
		ELSE from_unixtime(unix_timestamp(gen.fec_reestruc, 'yyyyMMdd'), 'yyyy-MM-dd')
	END,
	CASE
		WHEN LOWER(gen.cod_producto) = 'null' THEN NULL
		ELSE trim(gen.cod_producto)
	END,
	CASE
		WHEN LOWER(gen.cod_subprodu) = 'null' THEN NULL
		ELSE trim(gen.cod_subprodu)
	END,
	TRIM(RE.cod_contenido),
	CASE
		WHEN LOWER(RE.fec_data) = 'null' THEN NULL
		ELSE from_unixtime(unix_timestamp(RE.fec_data, 'yyyyMMdd'), 'yyyy-MM-dd')
	END,
	TRIM(RE.cod_pais),
	CAST(SUBSTR(RE.IDF_PERS_ODS, 6, 8) AS INT),
	cli.cod_vincula,
	cli.cod_tip_persona,
	concat(cli.nom_nombre, cli.nom_apellido_1),
	TRIM(RE.cod_entidad_espana),
	TRIM(RE.cod_producto_gest),
	TRIM(RE.cod_divisa),
	TRIM(RE.cod_reajuste),
	TRIM(RE.cod_agrp_func),
	TRIM(RE.cod_est_sdo),
	TRIM(RE.cod_cta_cont_gestion),
	TRIM(RE.cod_area_negocio),
	CASE
		WHEN ADN.cod_area_negocio_niv_9 = 'null' THEN NULL
		ELSE trim(ADN.cod_area_negocio_niv_9)
	END,
	CASE
		WHEN ADN.des_area_negocio_niv_9 = 'null' THEN NULL
		ELSE trim(ADN.des_area_negocio_niv_9)
	END,
	TRIM(RE.cod_tip_ajuste),
	TRIM(RE.cod_centro_cont),
	TRIM(RE.cod_ofi_comercial),
	TRIM(RE.ind_conciliacion),
	CASE
		WHEN TRIM(RE.ind_pool)= 'S' THEN 1
		ELSE 0
	END,
	TRIM(RE.cod_segmento_gest),
	TRIM(RE.cod_gestor),
	TRIM(RE.cod_univ),
	TRIM(RE.cod_gestor_prod),
	TRIM(RE.cod_origen_inf),
	TRIM(entj.cod_hijo),
	CASE
		WHEN LOWER(prod.cod_producto_niv_1) = 'null' THEN NULL
		ELSE trim(prod.cod_producto_niv_1)
	END,
	CASE
		WHEN LOWER(prod.cod_producto_niv_2) = 'null' THEN NULL
		ELSE trim(prod.cod_producto_niv_2)
	END,
	CASE
		WHEN LOWER(prod.cod_producto_niv_3) = 'null' THEN NULL
		ELSE trim(prod.cod_producto_niv_3)
	END,
	CASE
		WHEN LOWER(prod.cod_producto_niv_4) = 'null' THEN NULL
		ELSE trim(prod.cod_producto_niv_4)
	END,
	CASE
		WHEN LOWER(prod.cod_producto_niv_5) = 'null' THEN NULL
		ELSE trim(prod.cod_producto_niv_5)
	END,
	CASE
		WHEN LOWER(prod.cod_producto_niv_6) = 'null' THEN NULL
		ELSE trim(prod.cod_producto_niv_6)
	END,
	CASE
		WHEN LOWER(prod.cod_producto_niv_7) = 'null' THEN NULL
		ELSE trim(prod.cod_producto_niv_7)
	END,
	CASE
		WHEN LOWER(prod.cod_producto_niv_8) = 'null' THEN NULL
		ELSE trim(prod.cod_producto_niv_8)
	END,
	CASE
		WHEN LOWER(prod.cod_producto_niv_9) = 'null' THEN NULL
		ELSE trim(prod.cod_producto_niv_9)
	END,
	CASE
		WHEN LOWER(prod.cod_producto_niv_10) = 'null' THEN NULL
		ELSE trim(prod.cod_producto_niv_10)
	END,
	CASE
		WHEN LOWER(prod.des_producto_niv_1) = 'null' THEN NULL
		ELSE trim(prod.des_producto_niv_1)
	END,
	CASE
		WHEN LOWER(prod.des_producto_niv_2) = 'null' THEN NULL
		ELSE trim(prod.des_producto_niv_2)
	END,
	CASE
		WHEN LOWER(prod.des_producto_niv_3) = 'null' THEN NULL
		ELSE trim(prod.des_producto_niv_3)
	END,
	CASE
		WHEN LOWER(prod.des_producto_niv_4) = 'null' THEN NULL
		ELSE trim(prod.des_producto_niv_4)
	END,
	CASE
		WHEN LOWER(prod.des_producto_niv_5) = 'null' THEN NULL
		ELSE trim(prod.des_producto_niv_5)
	END,
	CASE
		WHEN LOWER(prod.des_producto_niv_6) = 'null' THEN NULL
		ELSE trim(prod.des_producto_niv_6)
	END,
	CASE
		WHEN LOWER(prod.des_producto_niv_7) = 'null' THEN NULL
		ELSE trim(prod.des_producto_niv_7)
	END,
	CASE
		WHEN LOWER(prod.des_producto_niv_8) = 'null' THEN NULL
		ELSE trim(prod.des_producto_niv_8)
	END,
	CASE
		WHEN LOWER(prod.des_producto_niv_9) = 'null' THEN NULL
		ELSE trim(prod.des_producto_niv_9)
	END,
	CASE
		WHEN LOWER(prod.des_producto_niv_10) = 'null' THEN NULL
		ELSE trim(prod.des_producto_niv_10)
	END,
	CASE
		WHEN LOWER(cta.cod_cta_resultados_niv_1) = 'null' THEN NULL
		ELSE trim(cta.cod_cta_resultados_niv_1)
	END ,
	CASE
		WHEN LOWER(cta.cod_cta_resultados_niv_2) = 'null' THEN NULL
		ELSE trim(cta.cod_cta_resultados_niv_2)
	END ,
	CASE
		WHEN LOWER(cta.cod_cta_resultados_niv_3) = 'null' THEN NULL
		ELSE trim(cta.cod_cta_resultados_niv_3)
	END ,
	CASE
		WHEN LOWER(cta.cod_cta_resultados_niv_4) = 'null' THEN NULL
		ELSE trim(cta.cod_cta_resultados_niv_4)
	END ,
	CASE
		WHEN LOWER(cta.cod_cta_resultados_niv_5) = 'null' THEN NULL
		ELSE trim(cta.cod_cta_resultados_niv_5)
	END ,
	CASE
		WHEN LOWER(cta.cod_cta_resultados_niv_6) = 'null' THEN NULL
		ELSE trim(cta.cod_cta_resultados_niv_6)
	END ,
	CASE
		WHEN LOWER(cta.cod_cta_resultados_niv_7) = 'null' THEN NULL
		ELSE trim(cta.cod_cta_resultados_niv_7)
	END ,
	CASE
		WHEN LOWER(cta.cod_cta_resultados_niv_8) = 'null' THEN NULL
		ELSE trim(cta.cod_cta_resultados_niv_8)
	END ,
	CASE
		WHEN LOWER(cta.cod_cta_resultados_niv_9) = 'null' THEN NULL
		ELSE trim(cta.cod_cta_resultados_niv_9)
	END ,
	CASE
		WHEN LOWER(cta.cod_cta_resultados_niv_10) = 'null' THEN NULL
		ELSE trim(cta.cod_cta_resultados_niv_10)
	END ,
	CASE
		WHEN LOWER(cta.cod_cta_resultados_niv_11) = 'null' THEN NULL
		ELSE trim(cta.cod_cta_resultados_niv_11)
	END ,
	CASE
		WHEN LOWER(cta.cod_cta_resultados_niv_12) = 'null' THEN NULL
		ELSE trim(cta.cod_cta_resultados_niv_12)
	END ,
	CASE
		WHEN LOWER(cta.des_cta_resultados_niv_1) = 'null' THEN NULL
		ELSE trim(cta.des_cta_resultados_niv_1)
	END ,
	CASE
		WHEN LOWER(cta.des_cta_resultados_niv_2) = 'null' THEN NULL
		ELSE trim(cta.des_cta_resultados_niv_2)
	END ,
	CASE
		WHEN LOWER(cta.des_cta_resultados_niv_3) = 'null' THEN NULL
		ELSE trim(cta.des_cta_resultados_niv_3)
	END ,
	CASE
		WHEN LOWER(cta.des_cta_resultados_niv_4) = 'null' THEN NULL
		ELSE trim(cta.des_cta_resultados_niv_4)
	END ,
	CASE
		WHEN LOWER(cta.des_cta_resultados_niv_5) = 'null' THEN NULL
		ELSE trim(cta.des_cta_resultados_niv_5)
	END ,
	CASE
		WHEN LOWER(cta.des_cta_resultados_niv_6) = 'null' THEN NULL
		ELSE trim(cta.des_cta_resultados_niv_6)
	END ,
	CASE
		WHEN LOWER(cta.des_cta_resultados_niv_7) = 'null' THEN NULL
		ELSE trim(cta.des_cta_resultados_niv_7)
	END ,
	CASE
		WHEN LOWER(cta.des_cta_resultados_niv_8) = 'null' THEN NULL
		ELSE trim(cta.des_cta_resultados_niv_8)
	END ,
	CASE
		WHEN LOWER(cta.des_cta_resultados_niv_9) = 'null' THEN NULL
		ELSE trim(cta.des_cta_resultados_niv_9)
	END ,
	CASE
		WHEN LOWER(cta.des_cta_resultados_niv_10) = 'null' THEN NULL
		ELSE trim(cta.des_cta_resultados_niv_10)
	END ,
	CASE
		WHEN LOWER(cta.des_cta_resultados_niv_11) = 'null' THEN NULL
		ELSE trim(cta.des_cta_resultados_niv_11)
	END ,
	CASE
		WHEN LOWER(cta.des_cta_resultados_niv_12) = 'null' THEN NULL
		ELSE trim(cta.des_cta_resultados_niv_12)
	END ,
	CASE
		WHEN LOWER(ADN.cod_area_negocio_niv_1) = 'null' THEN NULL
		ELSE trim(ADN.cod_area_negocio_niv_1)
	END ,
	CASE
		WHEN LOWER(ADN.cod_area_negocio_niv_2) = 'null' THEN NULL
		ELSE trim(ADN.cod_area_negocio_niv_2)
	END ,
	CASE
		WHEN LOWER(ADN.cod_area_negocio_niv_3) = 'null' THEN NULL
		ELSE trim(ADN.cod_area_negocio_niv_3)
	END ,
	CASE
		WHEN LOWER(ADN.cod_area_negocio_niv_4) = 'null' THEN NULL
		ELSE trim(ADN.cod_area_negocio_niv_4)
	END ,
	CASE
		WHEN LOWER(ADN.cod_area_negocio_niv_5) = 'null' THEN NULL
		ELSE trim(ADN.cod_area_negocio_niv_5)
	END ,
	CASE
		WHEN LOWER(ADN.cod_area_negocio_niv_6) = 'null' THEN NULL
		ELSE trim(ADN.cod_area_negocio_niv_6)
	END ,
	CASE
		WHEN LOWER(ADN.cod_area_negocio_niv_7) = 'null' THEN NULL
		ELSE trim(ADN.cod_area_negocio_niv_7)
	END ,
	CASE
		WHEN LOWER(ADN.des_area_negocio_niv_1) = 'null' THEN NULL
		ELSE trim(ADN.des_area_negocio_niv_1)
	END ,
	CASE
		WHEN LOWER(ADN.des_area_negocio_niv_2) = 'null' THEN NULL
		ELSE trim(ADN.des_area_negocio_niv_2)
	END ,
	CASE
		WHEN LOWER(ADN.des_area_negocio_niv_3) = 'null' THEN NULL
		ELSE trim(ADN.des_area_negocio_niv_3)
	END ,
	CASE
		WHEN LOWER(ADN.des_area_negocio_niv_4) = 'null' THEN NULL
		ELSE trim(ADN.des_area_negocio_niv_4)
	END ,
	CASE
		WHEN LOWER(ADN.des_area_negocio_niv_5) = 'null' THEN NULL
		ELSE trim(ADN.des_area_negocio_niv_5)
	END ,
	CASE
		WHEN LOWER(ADN.des_area_negocio_niv_6) = 'null' THEN NULL
		ELSE trim(ADN.des_area_negocio_niv_6)
	END ,
	CASE
		WHEN LOWER(ADN.des_area_negocio_niv_7) = 'null' THEN NULL
		ELSE trim(ADN.des_area_negocio_niv_7)
	END ,
	CASE
		WHEN LOWER(ENT.cod_entidad_gest_niv_1) = 'null' THEN NULL
		ELSE trim(ENT.cod_entidad_gest_niv_1)
	END ,
	CASE
		WHEN LOWER(ENT.cod_entidad_gest_niv_2) = 'null' THEN NULL
		ELSE trim(ENT.cod_entidad_gest_niv_2)
	END ,
	CASE
		WHEN LOWER(ENT.cod_entidad_gest_niv_3) = 'null' THEN NULL
		ELSE trim(ENT.cod_entidad_gest_niv_3)
	END ,
	CASE
		WHEN LOWER(ENT.cod_entidad_gest_niv_4) = 'null' THEN NULL
		ELSE trim(ENT.cod_entidad_gest_niv_4)
	END ,
	CASE
		WHEN LOWER(ENT.cod_entidad_gest_niv_5) = 'null' THEN NULL
		ELSE trim(ENT.cod_entidad_gest_niv_5)
	END ,
	CASE
		WHEN LOWER(ENT.cod_entidad_gest_niv_6) = 'null' THEN NULL
		ELSE trim(ENT.cod_entidad_gest_niv_6)
	END ,
	CASE
		WHEN LOWER(ENT.des_entidad_gest_niv_1) = 'null' THEN NULL
		ELSE trim(ENT.des_entidad_gest_niv_1)
	END ,
	CASE
		WHEN LOWER(ENT.des_entidad_gest_niv_2) = 'null' THEN NULL
		ELSE trim(ENT.des_entidad_gest_niv_2)
	END ,
	CASE
		WHEN LOWER(ENT.des_entidad_gest_niv_3) = 'null' THEN NULL
		ELSE trim(ENT.des_entidad_gest_niv_3)
	END ,
	CASE
		WHEN LOWER(ENT.des_entidad_gest_niv_4) = 'null' THEN NULL
		ELSE trim(ENT.des_entidad_gest_niv_4)
	END ,
	CASE
		WHEN LOWER(ENT.des_entidad_gest_niv_5) = 'null' THEN NULL
		ELSE trim(ENT.des_entidad_gest_niv_5)
	END ,
	CASE
		WHEN LOWER(ENT.des_entidad_gest_niv_6) = 'null' THEN NULL
		ELSE trim(ENT.des_entidad_gest_niv_6)
	END ,
	CASE
		WHEN LOWER(OFFI.cod_ofi_comercial_niv_5) = 'null' THEN NULL
		ELSE trim(OFFI.cod_ofi_comercial_niv_5)
	END ,
	CASE
		WHEN LOWER(OFFI.cod_ofi_comercial_niv_6) = 'null' THEN NULL
		ELSE trim(OFFI.cod_ofi_comercial_niv_6)
	END ,
	CASE
		WHEN LOWER(OFFI.cod_ofi_comercial_niv_7) = 'null' THEN NULL
		ELSE trim(OFFI.cod_ofi_comercial_niv_7)
	END ,
	CASE
		WHEN LOWER(OFFI.cod_ofi_comercial_niv_8) = 'null' THEN NULL
		ELSE trim(OFFI.cod_ofi_comercial_niv_8)
	END ,
	CASE
		WHEN LOWER(OFFI.cod_ofi_comercial_niv_9) = 'null' THEN NULL
		ELSE trim(OFFI.cod_ofi_comercial_niv_9)
	END ,
	CASE
		WHEN LOWER(OFFI.cod_ofi_comercial_niv_10) = 'null' THEN NULL
		ELSE trim(OFFI.cod_ofi_comercial_niv_10)
	END ,
	CASE
		WHEN LOWER(OFFI.cod_ofi_comercial_niv_11) = 'null' THEN NULL
		ELSE trim(OFFI.cod_ofi_comercial_niv_11)
	END ,
	CASE
		WHEN LOWER(OFFI.des_ofi_comercial_niv_5) = 'null' THEN NULL
		ELSE trim(OFFI.des_ofi_comercial_niv_5)
	END ,
	CASE
		WHEN LOWER(OFFI.des_ofi_comercial_niv_6) = 'null' THEN NULL
		ELSE trim(OFFI.des_ofi_comercial_niv_6)
	END ,
	CASE
		WHEN LOWER(OFFI.des_ofi_comercial_niv_7) = 'null' THEN NULL
		ELSE trim(OFFI.des_ofi_comercial_niv_7)
	END ,
	CASE
		WHEN LOWER(OFFI.des_ofi_comercial_niv_8) = 'null' THEN NULL
		ELSE trim(OFFI.des_ofi_comercial_niv_8)
	END ,
	CASE
		WHEN LOWER(OFFI.des_ofi_comercial_niv_9) = 'null' THEN NULL
		ELSE trim(OFFI.des_ofi_comercial_niv_9)
	END ,
	CASE
		WHEN LOWER(OFFI.des_ofi_comercial_niv_10) = 'null' THEN NULL
		ELSE trim(OFFI.des_ofi_comercial_niv_10)
	END ,
	CASE
		WHEN LOWER(OFFI.des_ofi_comercial_niv_11) = 'null' THEN NULL
		ELSE trim(OFFI.des_ofi_comercial_niv_11)
	END ,
	CASE
		WHEN LOWER(GEST.cod_gestor_niv_1) = 'null' THEN NULL
		ELSE trim(GEST.cod_gestor_niv_1)
	END ,
	CASE
		WHEN LOWER(GEST.cod_gestor_niv_2) = 'null' THEN NULL
		ELSE trim(GEST.cod_gestor_niv_2)
	END ,
	CASE
		WHEN LOWER(GEST.cod_gestor_niv_3) = 'null' THEN NULL
		ELSE trim(GEST.cod_gestor_niv_3)
	END ,
	CASE
		WHEN LOWER(GEST.cod_gestor_niv_4) = 'null' THEN NULL
		ELSE trim(GEST.cod_gestor_niv_4)
	END ,
	CASE
		WHEN LOWER(GEST.cod_gestor_niv_5) = 'null' THEN NULL
		ELSE trim(GEST.cod_gestor_niv_5)
	END ,
	CASE
		WHEN LOWER(GEST.cod_gestor_niv_6) = 'null' THEN NULL
		ELSE trim(GEST.cod_gestor_niv_6)
	END ,
	CASE
		WHEN LOWER(GEST.cod_gestor_niv_7) = 'null' THEN NULL
		ELSE trim(GEST.cod_gestor_niv_7)
	END ,
	CASE
		WHEN LOWER(GEST.cod_gestor_niv_8) = 'null' THEN NULL
		ELSE trim(GEST.cod_gestor_niv_8)
	END ,
	CASE
		WHEN LOWER(GEST.cod_gestor_niv_9) = 'null' THEN NULL
		ELSE trim(GEST.cod_gestor_niv_9)
	END ,
	CASE
		WHEN LOWER(GEST.cod_gestor_niv_10) = 'null' THEN NULL
		ELSE trim(GEST.cod_gestor_niv_10)
	END ,
	CASE
		WHEN LOWER(GEST.cod_gestor_niv_11) = 'null' THEN NULL
		ELSE trim(GEST.cod_gestor_niv_11)
	END ,
	CASE
		WHEN LOWER(GEST.des_gestor_niv_1) = 'null' THEN NULL
		ELSE trim(GEST.des_gestor_niv_1)
	END ,
	CASE
		WHEN LOWER(GEST.des_gestor_niv_2) = 'null' THEN NULL
		ELSE trim(GEST.des_gestor_niv_2)
	END ,
	CASE
		WHEN LOWER(GEST.des_gestor_niv_3) = 'null' THEN NULL
		ELSE trim(GEST.des_gestor_niv_3)
	END ,
	CASE
		WHEN LOWER(GEST.des_gestor_niv_4) = 'null' THEN NULL
		ELSE trim(GEST.des_gestor_niv_4)
	END ,
	CASE
		WHEN LOWER(GEST.des_gestor_niv_5) = 'null' THEN NULL
		ELSE trim(GEST.des_gestor_niv_5)
	END ,
	CASE
		WHEN LOWER(GEST.des_gestor_niv_6) = 'null' THEN NULL
		ELSE trim(GEST.des_gestor_niv_6)
	END ,
	CASE
		WHEN LOWER(GEST.des_gestor_niv_7) = 'null' THEN NULL
		ELSE trim(GEST.des_gestor_niv_7)
	END ,
	CASE
		WHEN LOWER(GEST.des_gestor_niv_8) = 'null' THEN NULL
		ELSE trim(GEST.des_gestor_niv_8)
	END ,
	CASE
		WHEN LOWER(GEST.des_gestor_niv_9) = 'null' THEN NULL
		ELSE trim(GEST.des_gestor_niv_9)
	END ,
	CASE
		WHEN LOWER(GEST.des_gestor_niv_10) = 'null' THEN NULL
		ELSE trim(GEST.des_gestor_niv_10)
	END ,
	CASE
		WHEN LOWER(GEST.des_gestor_niv_11) = 'null' THEN NULL
		ELSE trim(GEST.des_gestor_niv_11)
	END --,
	--RE.partition_date