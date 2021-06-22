set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=40000000;
-------------------------------------------------------------------
------------ b_result ---------------------------------------------
-------------------------------------------------------------------
CREATE TEMPORARY TABLE b_result AS
SELECT RE.idf_cto_ods cod_ren_contrato,
	TRIM(RE.cod_contenido) cod_ren_contenido,
	from_unixtime(unix_timestamp(RE.fec_data, 'yyyyMMdd'), 'yyyy-MM-dd') dt_ren_fec_data,
	TRIM(RE.cod_pais) cod_ren_pais,
	RE.idf_pers_ods ,
	CAST(SUBSTR(RE.idf_pers_ods, 6, 8) AS INT) cod_ren_nup,
	TRIM(RE.cod_entidad_espana) cod_ren_entidad_espana,
	TRIM(RE.cod_producto_gest) cod_ren_producto_gest,
	TRIM(RE.cod_divisa) cod_ren_divisa,
	TRIM(RE.cod_reajuste) cod_ren_reajuste,
	TRIM(RE.cod_agrp_func) cod_ren_agrp_func,
	TRIM(RE.cod_est_sdo) cod_ren_est_sdo,
	TRIM(RE.cod_cta_cont_gestion) cod_ren_cont_gestion,
	TRIM(RE.cod_area_negocio) cod_ren_area_negocio,
	TRIM(RE.cod_tip_ajuste) cod_ren_tipo_ajuste,
	TRIM(RE.cod_centro_cont) cod_ren_centro_cont,
	TRIM(RE.cod_ofi_comercial) cod_ren_ofi_comercial,
	TRIM(RE.ind_conciliacion) cod_ren_conciliacion,
	CASE WHEN TRIM(RE.ind_pool) = 'S' THEN 1 ELSE 0 END flag_ren_ind_pool,
	TRIM(RE.cod_segmento_gest) cod_ren_segmento_gest,
	TRIM(RE.cod_gestor) cod_ren_gestor,
	TRIM(RE.cod_univ) cod_ren_univ,
	TRIM(RE.cod_gestor_prod) cod_ren_gestor_prod,
	TRIM(RE.cod_origen_inf) cod_ren_origen_inf, 
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
	(sum(COALESCE(RE.IMP_SDO_CAP_MO, 0)) + sum(COALESCE(RE.IMP_SDO_INT_MO, 0)) + sum(COALESCE(RE.IMP_SDO_INSOLV_MO, 0)) + sum(COALESCE(RE.IMP_SDO_REAJUSTE_MO, 0))) AS fc_ren_saldo_puntual_MO ,
	RE.partition_date 
FROM bi_corp_staging.rio157_ms0_ft_dwh_blce_result RE 
WHERE RE.partition_date = '2020-05-31' 
GROUP BY RE.idf_cto_ods ,
	TRIM(RE.cod_contenido) ,
	from_unixtime(unix_timestamp(RE.fec_data, 'yyyyMMdd'), 'yyyy-MM-dd') ,
	TRIM(RE.cod_pais) ,
	RE.idf_pers_ods ,
	CAST(SUBSTR(RE.idf_pers_ods , 6, 8) AS INT) ,
	TRIM(RE.cod_entidad_espana) ,
	TRIM(RE.cod_producto_gest) ,
	TRIM(RE.cod_divisa) ,
	TRIM(RE.cod_reajuste) ,
	TRIM(RE.cod_agrp_func) ,
	TRIM(RE.cod_est_sdo) ,
	TRIM(RE.cod_cta_cont_gestion) ,
	TRIM(RE.cod_area_negocio) ,
	TRIM(RE.cod_tip_ajuste) ,
	TRIM(RE.cod_centro_cont) ,
	TRIM(RE.cod_ofi_comercial) ,
	TRIM(RE.ind_conciliacion) ,
	CASE WHEN TRIM(RE.ind_pool)= 'S' THEN 1 ELSE 0 	END ,
	TRIM(RE.cod_segmento_gest) ,
	TRIM(RE.cod_gestor) ,
	TRIM(RE.cod_univ) ,
	TRIM(RE.cod_gestor_prod) ,
	TRIM(RE.cod_origen_inf) ,
	RE.partition_date;	
-------------------------------------------------------------------
------------ blce_result ------------------------------------------
-------------------------------------------------------------------
CREATE TEMPORARY TABLE blce_result AS 
SELECT RE.cod_ren_contrato,
	from_unixtime(unix_timestamp(gen.fec_alta_cto , 'yyyyMMdd'), 'yyyy-MM-dd') dt_ren_fec_alta_cto,
	from_unixtime(unix_timestamp(gen.fec_ven, 'yyyyMMdd'), 'yyyy-MM-dd') dt_ren_fec_vto_cto,
	from_unixtime(unix_timestamp(gen.fec_reestruc, 'yyyyMMdd'), 'yyyy-MM-dd') dt_ren_fec_reestruc_cto,
	TRIM(gen.cod_producto) cod_ren_producto_generic,
	TRIM(gen.cod_subprodu) cod_ren_subproducto_generic,
	RE.cod_ren_contenido,
	RE.dt_ren_fec_data,
	RE.cod_ren_pais,
	RE.cod_ren_nup,
	cli.cod_vincula cod_ren_vincula,
	cli.cod_tip_persona cod_ren_tipo_persona,
	concat(cli.nom_nombre, cli.nom_apellido_1) ds_ren_nombre_cliente,
	RE.cod_ren_entidad_espana,
	RE.cod_ren_producto_gest,
	RE.cod_ren_divisa,
	RE.cod_ren_reajuste,
	RE.cod_ren_agrp_func,
	RE.cod_ren_est_sdo,
	RE.cod_ren_cont_gestion,
	RE.cod_ren_area_negocio,
	TRIM(ADN.cod_area_negocio_niv_9) cod_ren_division,
	TRIM(ADN.des_area_negocio_niv_9) ds_ren_division,
	RE.cod_ren_tipo_ajuste,
	RE.cod_ren_centro_cont,
	RE.cod_ren_ofi_comercial,
	RE.cod_ren_conciliacion,
	RE.flag_ren_ind_pool,
	RE.cod_ren_segmento_gest,
	RE.cod_ren_gestor,
	RE.cod_ren_univ,
	RE.cod_ren_gestor_prod,
	RE.cod_ren_origen_inf,
	TRIM(entj.cod_hijo) cod_ren_entidad_hijo,
	TRIM(prod.cod_producto_niv_1) cod_ren_producto_niv_1,
	TRIM(prod.cod_producto_niv_2) cod_ren_producto_niv_2,
	TRIM(prod.cod_producto_niv_3) cod_ren_producto_niv_3,
	TRIM(prod.cod_producto_niv_4) cod_ren_producto_niv_4,
	TRIM(prod.cod_producto_niv_5) cod_ren_producto_niv_5,
	TRIM(prod.cod_producto_niv_6) cod_ren_producto_niv_6,
	TRIM(prod.cod_producto_niv_7) cod_ren_producto_niv_7,
	TRIM(prod.cod_producto_niv_8) cod_ren_producto_niv_8,
	TRIM(prod.cod_producto_niv_9) cod_ren_producto_niv_9,
	TRIM(prod.cod_producto_niv_10) cod_ren_producto_niv_10,
	TRIM(prod.des_producto_niv_1) ds_ren_producto_niv_1,
	TRIM(prod.des_producto_niv_2) ds_ren_producto_niv_2,
	TRIM(prod.des_producto_niv_3) ds_ren_producto_niv_3,
	TRIM(prod.des_producto_niv_4) ds_ren_producto_niv_4,
	TRIM(prod.des_producto_niv_5) ds_ren_producto_niv_5,
	TRIM(prod.des_producto_niv_6) ds_ren_producto_niv_6,
	TRIM(prod.des_producto_niv_7) ds_ren_producto_niv_7,
	TRIM(prod.des_producto_niv_8) ds_ren_producto_niv_8,
	TRIM(prod.des_producto_niv_9) ds_ren_producto_niv_9,
	TRIM(prod.des_producto_niv_10) ds_ren_producto_niv_10,
	TRIM(cta.cod_cta_resultados_niv_1) cod_ren_cta_resultados_niv_1,
	TRIM(cta.cod_cta_resultados_niv_2) cod_ren_cta_resultados_niv_2,
	TRIM(cta.cod_cta_resultados_niv_3) cod_ren_cta_resultados_niv_3,
	TRIM(cta.cod_cta_resultados_niv_4) cod_ren_cta_resultados_niv_4,
	TRIM(cta.cod_cta_resultados_niv_5) cod_ren_cta_resultados_niv_5,
	TRIM(cta.cod_cta_resultados_niv_6) cod_ren_cta_resultados_niv_6,
	TRIM(cta.cod_cta_resultados_niv_7) cod_ren_cta_resultados_niv_7,
	TRIM(cta.cod_cta_resultados_niv_8) cod_ren_cta_resultados_niv_8,
	TRIM(cta.cod_cta_resultados_niv_9) cod_ren_cta_resultados_niv_9,
	TRIM(cta.cod_cta_resultados_niv_10) cod_ren_cta_resultados_niv_10,
	TRIM(cta.cod_cta_resultados_niv_11) cod_ren_cta_resultados_niv_11,
	TRIM(cta.cod_cta_resultados_niv_12) cod_ren_cta_resultados_niv_12,
	TRIM(cta.des_cta_resultados_niv_1) ds_ren_cta_resultados_niv_1,
	TRIM(cta.des_cta_resultados_niv_2) ds_ren_cta_resultados_niv_2,
	TRIM(cta.des_cta_resultados_niv_3) ds_ren_cta_resultados_niv_3,
	TRIM(cta.des_cta_resultados_niv_4) ds_ren_cta_resultados_niv_4,
	TRIM(cta.des_cta_resultados_niv_5) ds_ren_cta_resultados_niv_5,
	TRIM(cta.des_cta_resultados_niv_6) ds_ren_cta_resultados_niv_6,
	TRIM(cta.des_cta_resultados_niv_7) ds_ren_cta_resultados_niv_7,
	TRIM(cta.des_cta_resultados_niv_8) ds_ren_cta_resultados_niv_8,
	TRIM(cta.des_cta_resultados_niv_9) ds_ren_cta_resultados_niv_9,
	TRIM(cta.des_cta_resultados_niv_10) ds_ren_cta_resultados_niv_10,
	TRIM(cta.des_cta_resultados_niv_11) ds_ren_cta_resultados_niv_11,
	TRIM(cta.des_cta_resultados_niv_12) ds_ren_cta_resultados_niv_12,
	TRIM(ADN.cod_area_negocio_niv_1) cod_ren_area_negocio_niv_1,
	TRIM(ADN.cod_area_negocio_niv_2) cod_ren_area_negocio_niv_2,
	TRIM(ADN.cod_area_negocio_niv_3) cod_ren_area_negocio_niv_3,
	TRIM(ADN.cod_area_negocio_niv_4) cod_ren_area_negocio_niv_4,
	TRIM(ADN.cod_area_negocio_niv_5) cod_ren_area_negocio_niv_5,
	TRIM(ADN.cod_area_negocio_niv_6) cod_ren_area_negocio_niv_6,
	TRIM(ADN.cod_area_negocio_niv_7) cod_ren_area_negocio_niv_7,
	TRIM(ADN.des_area_negocio_niv_1) ds_ren_area_negocio_niv_1,
	TRIM(ADN.des_area_negocio_niv_2) ds_ren_area_negocio_niv_2,
	TRIM(ADN.des_area_negocio_niv_3) ds_ren_area_negocio_niv_3,
	TRIM(ADN.des_area_negocio_niv_4) ds_ren_area_negocio_niv_4,
	TRIM(ADN.des_area_negocio_niv_5) ds_ren_area_negocio_niv_5,
	TRIM(ADN.des_area_negocio_niv_6) ds_ren_area_negocio_niv_6,
	TRIM(ADN.des_area_negocio_niv_7) ds_ren_area_negocio_niv_7,
	TRIM(ENT.cod_entidad_gest_niv_1) cod_ren_entidad_gest_niv_1,
	TRIM(ENT.cod_entidad_gest_niv_2) cod_ren_entidad_gest_niv_2,
	TRIM(ENT.cod_entidad_gest_niv_3) cod_ren_entidad_gest_niv_3,
	TRIM(ENT.cod_entidad_gest_niv_4) cod_ren_entidad_gest_niv_4,
	TRIM(ENT.cod_entidad_gest_niv_5) cod_ren_entidad_gest_niv_5,
	TRIM(ENT.cod_entidad_gest_niv_6) cod_ren_entidad_gest_niv_6,
	TRIM(ENT.des_entidad_gest_niv_1) ds_ren_entidad_gest_niv_1,
	TRIM(ENT.des_entidad_gest_niv_2) ds_ren_entidad_gest_niv_2,
	TRIM(ENT.des_entidad_gest_niv_3) ds_ren_entidad_gest_niv_3,
	TRIM(ENT.des_entidad_gest_niv_4) ds_ren_entidad_gest_niv_4,
	TRIM(ENT.des_entidad_gest_niv_5) ds_ren_entidad_gest_niv_5,
	TRIM(ENT.des_entidad_gest_niv_6) ds_ren_entidad_gest_niv_6,
	TRIM(OFFI.cod_ofi_comercial_niv_5) cod_ren_ofi_comercial_niv_5,
	TRIM(OFFI.cod_ofi_comercial_niv_6) cod_ren_ofi_comercial_niv_6,
	TRIM(OFFI.cod_ofi_comercial_niv_7) cod_ren_ofi_comercial_niv_7,
	TRIM(OFFI.cod_ofi_comercial_niv_8) cod_ren_ofi_comercial_niv_8,
	TRIM(OFFI.cod_ofi_comercial_niv_9) cod_ren_ofi_comercial_niv_9,
	TRIM(OFFI.cod_ofi_comercial_niv_10) cod_ren_ofi_comercial_niv_10,
	TRIM(OFFI.cod_ofi_comercial_niv_11) cod_ren_ofi_comercial_niv_11,
	TRIM(OFFI.des_ofi_comercial_niv_5) ds_ren_ofi_comercial_niv_5,
	TRIM(OFFI.des_ofi_comercial_niv_6) ds_ren_ofi_comercial_niv_6,
	TRIM(OFFI.des_ofi_comercial_niv_7) ds_ren_ofi_comercial_niv_7,
	TRIM(OFFI.des_ofi_comercial_niv_8) ds_ren_ofi_comercial_niv_8,
	TRIM(OFFI.des_ofi_comercial_niv_9) ds_ren_ofi_comercial_niv_9,
	TRIM(OFFI.des_ofi_comercial_niv_10) ds_ren_ofi_comercial_niv_10,
	TRIM(OFFI.des_ofi_comercial_niv_11) ds_ren_ofi_comercial_niv_11,
	TRIM(GEST.cod_gestor_niv_1) cod_ren_gestor_niv_1,
	TRIM(GEST.cod_gestor_niv_2) cod_ren_gestor_niv_2,
	TRIM(GEST.cod_gestor_niv_3) cod_ren_gestor_niv_3,
	TRIM(GEST.cod_gestor_niv_4) cod_ren_gestor_niv_4,
	TRIM(GEST.cod_gestor_niv_5) cod_ren_gestor_niv_5,
	TRIM(GEST.cod_gestor_niv_6) cod_ren_gestor_niv_6,
	TRIM(GEST.cod_gestor_niv_7) cod_ren_gestor_niv_7,
	TRIM(GEST.cod_gestor_niv_8) cod_ren_gestor_niv_8,
	TRIM(GEST.cod_gestor_niv_9) cod_ren_gestor_niv_9,
	TRIM(GEST.cod_gestor_niv_10) cod_ren_gestor_niv_10,
	TRIM(GEST.cod_gestor_niv_11) cod_ren_gestor_niv_11,
	TRIM(GEST.des_gestor_niv_1) ds_ren_gestor_niv_1,
	TRIM(GEST.des_gestor_niv_2) ds_ren_gestor_niv_2,
	TRIM(GEST.des_gestor_niv_3) ds_ren_gestor_niv_3,
	TRIM(GEST.des_gestor_niv_4) ds_ren_gestor_niv_4,
	TRIM(GEST.des_gestor_niv_5) ds_ren_gestor_niv_5,
	TRIM(GEST.des_gestor_niv_6) ds_ren_gestor_niv_6,
	TRIM(GEST.des_gestor_niv_7) ds_ren_gestor_niv_7,
	TRIM(GEST.des_gestor_niv_8) ds_ren_gestor_niv_8,
	TRIM(GEST.des_gestor_niv_9) ds_ren_gestor_niv_9,
	TRIM(GEST.des_gestor_niv_10) ds_ren_gestor_niv_10,
	TRIM(GEST.des_gestor_niv_11) ds_ren_gestor_niv_11,
	RE.fc_ren_resultado_saldo_ficticio,
	RE.fc_ren_imp_ing_per_ml,
	RE.fc_ren_imp_ing_cap_ml,
	RE.fc_ren_ing_reaj_ml,
	RE.fc_ren_egr_per_ml,
	RE.fc_ren_egr_cap_ml,
	RE.fc_ren_egr_reaj_ml,
	RE.fc_ren_ajtti_egr_tb_cap_ml,
	RE.fc_ren_ajtti_egr_sl_cap_ml,
	RE.fc_ren_ajtti_egr_per_ml,
	RE.fc_ren_ajtti_egr_reajuste_ml,
	RE.fc_ren_ajtti_ing_tb_cap_ml,
	RE.fc_ren_ajtti_ing_sl_cap_ml,
	RE.fc_ren_ajtti_ing_per_ml,
	RE.fc_ren_ajtti_ing_reajuste_ml,
	RE.fc_ren_efec_enc_ml,
	RE.fc_ren_ing_per_mo,
	RE.fc_ren_ing_cap_mo,
	RE.fc_ren_ing_reaj_mo,
	RE.fc_ren_egr_per_mo,
	RE.fc_ren_egr_cap_mo,
	RE.fc_ren_egr_reaj_mo,
	RE.fc_ren_ajtti_egr_tb_cap_mo,
	RE.fc_ren_ajtti_egr_sl_cap_mo,
	RE.fc_ren_ajtti_egr_per_mo,
	RE.fc_ren_ajtti_egr_reajuste_mo,
	RE.fc_ren_ajtti_ing_tb_cap_mo,
	RE.fc_ren_ajtti_ing_sl_cap_mo,
	RE.fc_ren_ajtti_ing_per_mo,
	RE.fc_ren_ajtti_ing_reajuste_mo,
	RE.fc_ren_efec_enc_mo,
	RE.fc_ren_sdo_cap_med_ml,
	RE.fc_ren_sdo_med_int_ml,
	RE.fc_ren_sdo_med_reajuste_ml,
	RE.fc_ren_sdo_med_insolv_ml,
	RE.fc_ren_sdo_cap_med_mo,
	RE.fc_ren_sdo_med_int_mo,
	RE.fc_ren_sdo_med_reajuste_mo,
	RE.fc_ren_sdo_med_insolv_mo,
	RE.fc_ren_ing_per_ml,
	RE.fc_ren_restultado_total_ML,
	RE.fc_ren_restultado_total_MO,
	RE.fc_ren_saldo_medio_total_ML,
	RE.fc_ren_saldo_medio_total_MO,
	RE.fc_ren_resultado_total_real_ML,
	RE.fc_ren_resultado_total_real_MO,
	RE.fc_ren_saldo_puntual_ML,
	RE.fc_ren_saldo_puntual_MO,
	RE.partition_date
FROM b_result RE 
LEFT JOIN bi_corp_staging.rio157_ms0_dm_dwh_productos_ctr prod ON
	RE.cod_ren_producto_gest = prod.cod_producto
	AND prod.partition_date = '2020-07-14'
	AND prod.cod_jerq_pr01 = 'JBLDN'
LEFT JOIN bi_corp_staging.rio157_ms0_dm_dwh_area_negocio_ctr adn ON
	RE.cod_ren_area_negocio = adn.cod_area_negocio
	AND adn.partition_date = '2020-07-14'
	AND adn.cod_jerq_adn01 = 'JAN01'
	AND RE.cod_ren_pais = adn.cod_pais
LEFT JOIN bi_corp_staging.rio157_ms0_dm_dwh_cta_resultados_ctr cta ON
	RE.cod_ren_cont_gestion = cta.cod_cta_resultados
	AND cta.partition_date = '2020-07-10'
	AND cta.COD_JERQ_CR01 = 'JCNBV'
	AND RE.cod_ren_pais = cta.cod_pais
LEFT JOIN bi_corp_staging.rio157_ms0_dm_dwh_entidad_juridica_ ent ON
	RE.cod_ren_entidad_espana = ent.cod_entidad_gest
	AND ent.partition_date = '2020-05-30'
	AND ent.cod_jerq_ej01 = 'JEJ01'
	AND RE.cod_ren_pais = ent.cod_pais
LEFT JOIN bi_corp_staging.rio157_ms0_dm_dwh_oficina offi ON
	RE.cod_ren_ofi_comercial = offi.cod_ofi_comercial
	AND offi.partition_date = '2020-05-30'
	AND offi.cod_jerq_oc01 = 'JOC01'
	AND RE.cod_ren_pais = offi.cod_pais
LEFT JOIN bi_corp_staging.rio157_ms0_dm_dwh_gestor_ gest ON
	RE.cod_ren_gestor = gest.cod_gestor
	AND gest.partition_date = '2020-05-30'
	AND RE.cod_ren_pais = gest.cod_pais
LEFT JOIN bi_corp_staging.rio157_ms0_dt_dwh_cliente_result cli ON
	RE.IDF_PERS_ODS = cli.idf_pers_ods
	AND cli.partition_date = '2020-05-31'
	AND RE.cod_ren_pais = cli.cod_pais
LEFT JOIN bi_corp_staging.rio157_ms0_dt_dwh_generic_result gen ON
	RE.cod_ren_contrato = gen.idf_cto_ods
	AND gen.partition_date = '2020-05-31'
	AND gen.cod_contenido = RE.cod_ren_contenido
	AND RE.cod_ren_pais = gen.cod_pais
LEFT JOIN bi_corp_staging.rio157_ms0_dm_je_dwh_entidades_ctr entj ON
	RE.cod_ren_pais = entj.cod_pais
	AND entj.partition_date = '2020-07-14';
-------------------------------------------------------------------
------------ insert table -----------------------------------------
-------------------------------------------------------------------
INSERT OVERWRITE TABLE bi_corp_common.bt_ren_blce_result
PARTITION(partition_date)
SELECT * FROM blce_result;