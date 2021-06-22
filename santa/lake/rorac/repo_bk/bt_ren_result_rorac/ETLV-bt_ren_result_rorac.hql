set mapred.job.queue.name=root.dataeng;
set hive.ignore.mapjoin.hint=false;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=40000000;

-------------------------------------------------------------------
------------ GENERIC RESULT ---------------------------------------
-------------------------------------------------------------------
CREATE TEMPORARY TABLE generic_result AS
-- contratos data
SELECT DISTINCT TRIM(idf_cto_ods) idf_cto_ods
	, TRIM(cod_contenido) cod_contenido
	, TRIM(cod_pais) cod_pais
	, from_unixtime(unix_timestamp(fec_alta_cto , 'yyyyMMdd'), 'yyyy-MM-dd') fec_alta_cto
	, from_unixtime(unix_timestamp(fec_ven, 'yyyyMMdd'), 'yyyy-MM-dd') fec_ven
	, from_unixtime(unix_timestamp(fec_reestruc, 'yyyyMMdd'), 'yyyy-MM-dd') fec_reestruc
	, TRIM(cod_producto) cod_producto
	, TRIM(cod_subprodu) cod_subprodu
FROM bi_corp_staging.rio157_ms0_dt_dwh_generic_result 
WHERE partition_date = '2020-06-30' 
DISTRIBUTE BY idf_cto_ods ,cod_contenido 
SORT BY idf_cto_ods ,cod_contenido;

-------------------------------------------------------------------
------------ CLIENTE RESULT ---------------------------------------
-------------------------------------------------------------------
CREATE TEMPORARY TABLE cliente_result AS
SELECT DISTINCT idf_pers_ods
	, cod_vincula
	, cod_tip_persona
	, CONCAT(nom_nombre,' ', nom_apellido_1) nombre_cliente
FROM bi_corp_staging.rio157_ms0_dt_dwh_cliente_result
WHERE partition_date = '2020-06-30' 
SORT BY idf_pers_ods, cod_vincula, cod_tip_persona ;

-------------------------------------------------------------------
------------ BLCE_RESULT ------------------------------------------
-------------------------------------------------------------------
CREATE TEMPORARY TABLE blce_result AS 
SELECT RE.idf_cto_ods ,
	RE.cod_contenido ,
	from_unixtime(unix_timestamp(RE.fec_data , 'yyyyMMdd'), 'yyyy-MM-dd') fec_data,
	RE.cod_pais ,
	RE.idf_pers_ods ,
	CAST(SUBSTR(RE.idf_pers_ods, 6, 8) AS INT) per_nup,
	RE.cod_entidad_espana ,
	RE.cod_producto_gest,
	RE.cod_divisa,
	RE.cod_reajuste,
	RE.cod_agrp_func,
	RE.cod_est_sdo,
	RE.cod_cta_cont_gestion,
	RE.cod_area_negocio,
	RE.cod_tip_ajuste,
	RE.cod_centro_cont,
	RE.cod_ofi_comercial,
	RE.ind_conciliacion,
	CASE WHEN TRIM(RE.ind_pool) = 'S' THEN 1 ELSE 0 END flag_ren_ind_pool,
	RE.cod_segmento_gest,
	RE.cod_gestor,
	RE.cod_univ,
	RE.cod_gestor_prod,
	RE.cod_origen_inf, 
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
	(sum(COALESCE(RE.IMP_ING_PER_ML, 0)) 
	+ sum(COALESCE(RE.IMP_ING_CAP_ML, 0)) 
	+ sum(COALESCE(RE.IMP_ING_REAJ_ML, 0)) 
	+ sum(COALESCE(RE.IMP_EGR_PER_ML, 0)) 
	+ sum(COALESCE(RE.IMP_EGR_CAP_ML, 0)) 
	+ sum(COALESCE(RE.IMP_EGR_REAJ_ML, 0)) 
	+ sum(COALESCE(RE.IMP_AJTTI_EGR_TB_CAP_ML, 0)) 
	+ sum(COALESCE(RE.IMP_AJTTI_EGR_SL_CAP_ML, 0)) 
	+ sum(COALESCE(RE.IMP_AJTTI_EGR_PER_ML, 0)) 
	+ sum(COALESCE(RE.IMP_AJTTI_EGR_REAJUSTE_ML, 0)) 
	+ sum(COALESCE(RE.IMP_AJTTI_ING_TB_CAP_ML, 0)) 
	+ sum(COALESCE(RE.IMP_AJTTI_ING_SL_CAP_ML, 0)) 
	+ sum(COALESCE(RE.IMP_AJTTI_ING_PER_ML, 0)) 
	+ sum(COALESCE(RE.IMP_AJTTI_ING_REAJUSTE_ML, 0)) 
	+ sum(COALESCE(RE.IMP_EFEC_ENC_ML, 0))) AS fc_ren_restultado_total_ML, 
	(sum(COALESCE(RE.IMP_ING_PER_MO, 0)) 
	+ sum(COALESCE(RE.IMP_ING_CAP_MO, 0)) 
	+ sum(COALESCE(RE.IMP_ING_REAJ_MO, 0)) 
	+ sum(COALESCE(RE.IMP_EGR_PER_MO, 0)) 
	+ sum(COALESCE(RE.IMP_EGR_CAP_ML, 0)) 
	+ sum(COALESCE(RE.IMP_EGR_REAJ_MO, 0)) 
	+ sum(COALESCE(RE.IMP_AJTTI_EGR_TB_CAP_MO, 0)) 
	+ sum(COALESCE(RE.IMP_AJTTI_EGR_SL_CAP_MO, 0)) 
	+ sum(COALESCE(RE.IMP_AJTTI_EGR_PER_MO, 0)) 
	+ sum(COALESCE(RE.IMP_AJTTI_EGR_REAJUSTE_MO, 0)) 
	+ sum(COALESCE(RE.IMP_AJTTI_ING_TB_CAP_MO, 0)) 
	+ sum(COALESCE(RE.IMP_AJTTI_ING_SL_CAP_MO, 0)) 
	+ sum(COALESCE(RE.IMP_AJTTI_ING_PER_MO, 0)) 
	+ sum(COALESCE(RE.IMP_AJTTI_ING_REAJUSTE_MO, 0)) 
	+ sum(COALESCE(RE.IMP_EFEC_ENC_MO, 0))) AS fc_ren_restultado_total_MO, 
	(sum(COALESCE(RE.IMP_SDO_CAP_MED_ML, 0)) 
	+ sum(COALESCE(RE.IMP_SDO_MED_INT_ML, 0)) 
	+ sum(COALESCE(RE.IMP_SDO_MED_REAJUSTE_ML, 0)) 
	+ sum(COALESCE(RE.IMP_SDO_MED_INSOLV_ML, 0))) AS fc_ren_saldo_medio_total_ML, 
	(sum(COALESCE(RE.IMP_SDO_CAP_MED_MO, 0)) 
	+ sum(COALESCE(RE.IMP_SDO_MED_INT_MO, 0)) 
	+ sum(COALESCE(RE.IMP_SDO_MED_REAJUSTE_MO, 0)) 
	+ sum(COALESCE(RE.IMP_SDO_MED_INSOLV_MO, 0))) AS fc_ren_saldo_medio_total_MO, 
	(sum(COALESCE(RE.IMP_ING_PER_ML, 0)) 
	+ sum(COALESCE(RE.IMP_ING_CAP_ML, 0)) 
	+ sum(COALESCE(RE.IMP_ING_REAJ_ML, 0)) 
	+ sum(COALESCE(RE.IMP_EGR_PER_ML, 0)) 
	+ sum(COALESCE(RE.IMP_EGR_CAP_ML, 0)) 
	+ sum(COALESCE(RE.IMP_EGR_REAJ_ML, 0))) AS fc_ren_resultado_total_real_ML,
	(sum(COALESCE(RE.IMP_ING_PER_MO, 0)) 
	+ sum(COALESCE(RE.IMP_ING_CAP_MO, 0)) 
	+ sum(COALESCE(RE.IMP_ING_REAJ_MO, 0)) 
	+ sum(COALESCE(RE.IMP_EGR_PER_MO, 0)) 
	+ sum(COALESCE(RE.IMP_EGR_CAP_MO, 0)) 
	+ sum(COALESCE(RE.IMP_EGR_REAJ_MO, 0))) AS fc_ren_resultado_total_real_MO,
	(sum(COALESCE(RE.IMP_SDO_CAP_ML, 0)) 
	+ sum(COALESCE(RE.IMP_SDO_INT_ML, 0)) 
	+ sum(COALESCE(RE.IMP_SDO_INSOLV_ML, 0)) 
	+ sum(COALESCE(RE.IMP_SDO_REAJUSTE_ML, 0))) AS fc_ren_saldo_puntual_ML,
	(sum(COALESCE(RE.IMP_SDO_CAP_MO, 0)) 
	+ sum(COALESCE(RE.IMP_SDO_INT_MO, 0)) 
	+ sum(COALESCE(RE.IMP_SDO_INSOLV_MO, 0)) 
	+ sum(COALESCE(RE.IMP_SDO_REAJUSTE_MO, 0))) AS fc_ren_saldo_puntual_MO ,
	RE.partition_date 
FROM bi_corp_staging.rio157_ms0_ft_dwh_blce_result RE 
WHERE RE.partition_date = '2020-06-30' 
GROUP BY RE.idf_cto_ods ,
	RE.cod_contenido ,
	RE.fec_data ,
	RE.cod_pais ,
	RE.idf_pers_ods ,
	CAST(SUBSTR(RE.idf_pers_ods, 6, 8) AS INT) ,
	RE.cod_entidad_espana ,
	RE.cod_producto_gest ,
	RE.cod_divisa ,
	RE.cod_reajuste ,
	RE.cod_agrp_func ,
	RE.cod_est_sdo ,
	RE.cod_cta_cont_gestion ,
	RE.cod_area_negocio ,
	RE.cod_tip_ajuste ,
	RE.cod_centro_cont ,
	RE.cod_ofi_comercial ,
	RE.ind_conciliacion ,
	CASE WHEN TRIM(RE.ind_pool) = 'S' THEN 1 ELSE 0 END ,
	RE.cod_segmento_gest ,
	RE.cod_gestor ,
	RE.cod_univ ,
	RE.cod_gestor_prod ,
	RE.cod_origen_inf ,
	RE.partition_date 
DISTRIBUTE BY RE.idf_cto_ods ,RE.cod_contenido 
SORT BY RE.idf_cto_ods ,RE.cod_contenido;

-------------------------------------------------------------------
------------ RESULT G  --------------------------------------------
-------------------------------------------------------------------	
CREATE TEMPORARY TABLE blce_result_g AS
SELECT RE.*
	, gen.cod_producto
	, gen.cod_subprodu
	, gen.fec_alta_cto
	, gen.fec_ven
	, gen.fec_reestruc
FROM blce_result RE
LEFT JOIN generic_result gen ON 
	RE.idf_cto_ods = gen.idf_cto_ods
	AND RE.cod_contenido = gen.cod_contenido 
DISTRIBUTE BY RE.idf_cto_ods ,RE.cod_contenido 
SORT BY RE.idf_cto_ods ,RE.cod_contenido;

-------------------------------------------------------------------
------------ RESULT GC --------------------------------------------
-------------------------------------------------------------------
CREATE TEMPORARY TABLE blce_result_gc AS
SELECT RE.*	
	, cli.cod_vincula
	, cli.cod_tip_persona
	, cli.nombre_cliente
FROM blce_result_g RE
LEFT JOIN cliente_result cli ON
	cli.idf_pers_ods = RE.idf_pers_ods 
DISTRIBUTE BY RE.idf_cto_ods ,RE.idf_pers_ods 
SORT BY RE.idf_cto_ods ,RE.idf_pers_ods;

-------------------------------------------------------------------
------------ DROP TEMP --------------------------------------------
-------------------------------------------------------------------
DROP TABLE IF EXISTS generic_result ;
DROP TABLE IF EXISTS cliente_result ;
DROP TABLE IF EXISTS blce_result ;
DROP TABLE IF EXISTS blce_result_g ;

---------------------------------------------------------------------------------
------------ RESULT JOIN ADN ----------------------------------------------------
---------------------------------------------------------------------------------
CREATE TEMPORARY TABLE blce_result_adn AS
SELECT /*+ MAPJOIN(bi_corp_common.dim_ren_area_de_negocio_ctr) */
	RE.* ,
	adn.cod_ren_area_negocio_niv_9 AS cod_ren_division,
	adn.ds_ren_area_negocio_niv_9 AS ds_ren_division,
	adn.cod_ren_area_negocio_niv_1,
	adn.cod_ren_area_negocio_niv_2,
	adn.cod_ren_area_negocio_niv_3,
	adn.cod_ren_area_negocio_niv_4,
	adn.cod_ren_area_negocio_niv_5,
	adn.cod_ren_area_negocio_niv_6,
	adn.cod_ren_area_negocio_niv_7,
	adn.ds_ren_area_negocio_niv_1,
	adn.ds_ren_area_negocio_niv_2,
	adn.ds_ren_area_negocio_niv_3,
	adn.ds_ren_area_negocio_niv_4,
	adn.ds_ren_area_negocio_niv_5,
	adn.ds_ren_area_negocio_niv_6,
	adn.ds_ren_area_negocio_niv_7
FROM blce_result_gc RE
LEFT JOIN bi_corp_common.dim_ren_area_de_negocio_ctr adn ON
	RE.cod_area_negocio = adn.cod_ren_area_negocio 
DISTRIBUTE BY RE.cod_area_negocio 
SORT BY RE.cod_area_negocio;

-------------------------------------------------------------------
------------ DROP TEMP --------------------------------------------
-------------------------------------------------------------------
DROP TABLE IF EXISTS blce_result_gc ;

---------------------------------------------------------------------------------
------------ RESULT JOIN PROD ---------------------------------------------------
---------------------------------------------------------------------------------
CREATE TEMPORARY TABLE blce_result_prod AS
SELECT /*+ MAPJOIN(bi_corp_common.dim_ren_productos_ctr) */
	RE.* ,
	prod.cod_ren_producto_niv_1,
	prod.cod_ren_producto_niv_2,
	prod.cod_ren_producto_niv_3,
	prod.cod_ren_producto_niv_4,
	prod.cod_ren_producto_niv_5,
	prod.cod_ren_producto_niv_6,
	prod.cod_ren_producto_niv_7,
	prod.cod_ren_producto_niv_8,
	prod.cod_ren_producto_niv_9,
	prod.cod_ren_producto_niv_10,
	prod.ds_ren_producto_niv_1,
	prod.ds_ren_producto_niv_2,
	prod.ds_ren_producto_niv_3,
	prod.ds_ren_producto_niv_4,
	prod.ds_ren_producto_niv_5,
	prod.ds_ren_producto_niv_6,
	prod.ds_ren_producto_niv_7,
	prod.ds_ren_producto_niv_8,
	prod.ds_ren_producto_niv_9,
	prod.ds_ren_producto_niv_10
FROM blce_result_adn RE
LEFT JOIN bi_corp_common.dim_ren_productos_ctr prod ON
	RE.cod_producto_gest = prod.cod_ren_producto 
DISTRIBUTE BY RE.cod_producto_gest 
SORT BY RE.cod_producto_gest;

-------------------------------------------------------------------
------------ DROP TEMP --------------------------------------------
-------------------------------------------------------------------
DROP TABLE IF EXISTS blce_result_adn ;

---------------------------------------------------------------------------------
------------ RESULT JOIN CTA ----------------------------------------------------
---------------------------------------------------------------------------------
CREATE TEMPORARY TABLE blce_result_cta AS
SELECT /*+ MAPJOIN(bi_corp_common.dim_ren_cta_resultados_ctr) */
	RE.* ,
	cta.cod_ren_cta_resultados_niv_1,
	cta.cod_ren_cta_resultados_niv_2,
	cta.cod_ren_cta_resultados_niv_3,
	cta.cod_ren_cta_resultados_niv_4,
	cta.cod_ren_cta_resultados_niv_5,
	cta.cod_ren_cta_resultados_niv_6,
	cta.cod_ren_cta_resultados_niv_7,
	cta.cod_ren_cta_resultados_niv_8,
	cta.cod_ren_cta_resultados_niv_9,
	cta.cod_ren_cta_resultados_niv_10,
	cta.cod_ren_cta_resultados_niv_11,
	cta.cod_ren_cta_resultados_niv_12,
	cta.ds_ren_cta_resultados_niv_1,
	cta.ds_ren_cta_resultados_niv_2,
	cta.ds_ren_cta_resultados_niv_3,
	cta.ds_ren_cta_resultados_niv_4,
	cta.ds_ren_cta_resultados_niv_5,
	cta.ds_ren_cta_resultados_niv_6,
	cta.ds_ren_cta_resultados_niv_7,
	cta.ds_ren_cta_resultados_niv_8,
	cta.ds_ren_cta_resultados_niv_9,
	cta.ds_ren_cta_resultados_niv_10,
	cta.ds_ren_cta_resultados_niv_11,
	cta.ds_ren_cta_resultados_niv_12
FROM blce_result_prod RE
LEFT JOIN bi_corp_common.dim_ren_cta_resultados_ctr cta ON
	RE.cod_cta_cont_gestion = cta.cod_ren_cta_resultados 
DISTRIBUTE BY RE.cod_cta_cont_gestion 
SORT BY RE.cod_cta_cont_gestion;

-------------------------------------------------------------------
------------ DROP TEMP --------------------------------------------
-------------------------------------------------------------------
DROP TABLE IF EXISTS blce_result_prod ;

---------------------------------------------------------------------------------
------------ RESULT JOIN ENT ----------------------------------------------------
---------------------------------------------------------------------------------
CREATE TEMPORARY TABLE blce_result_ent AS
SELECT /*+ MAPJOIN(bi_corp_common.dim_ren_entidad_juridica) */
	RE.* ,
	ent.cod_ren_entidad_gest_niv_1,
	ent.cod_ren_entidad_gest_niv_2,
	ent.cod_ren_entidad_gest_niv_3,
	ent.cod_ren_entidad_gest_niv_4,
	ent.cod_ren_entidad_gest_niv_5,
	ent.cod_ren_entidad_gest_niv_6,
	ent.ds_ren_entidad_gest_niv_1,
	ent.ds_ren_entidad_gest_niv_2,
	ent.ds_ren_entidad_gest_niv_3,
	ent.ds_ren_entidad_gest_niv_4,
	ent.ds_ren_entidad_gest_niv_5,
	ent.ds_ren_entidad_gest_niv_6
FROM blce_result_cta RE
LEFT JOIN bi_corp_common.dim_ren_entidad_juridica ent ON
	RE.cod_entidad_espana = ent.cod_ren_entidad_gest 
DISTRIBUTE BY RE.cod_entidad_espana 
SORT BY RE.cod_entidad_espana;

-------------------------------------------------------------------
------------ DROP TEMP --------------------------------------------
-------------------------------------------------------------------
DROP TABLE IF EXISTS blce_result_cta ;

---------------------------------------------------------------------------------
------------ RESULT JOIN OFFI ---------------------------------------------------
---------------------------------------------------------------------------------
CREATE TEMPORARY TABLE blce_result_offi AS
SELECT /*+ MAPJOIN(bi_corp_common.dim_ren_oficinas) */
	RE.* ,
	offi.cod_ren_ofi_comercial_niv_5 ,
	offi.cod_ren_ofi_comercial_niv_6 ,
	offi.cod_ren_ofi_comercial_niv_7 ,
	offi.cod_ren_ofi_comercial_niv_8 ,
	offi.cod_ren_ofi_comercial_niv_9 ,
	offi.cod_ren_ofi_comercial_niv_10 ,
	offi.cod_ren_ofi_comercial_niv_11 ,
	offi.ds_ren_ofi_comercial_niv_5 ,
	offi.ds_ren_ofi_comercial_niv_6 ,
	offi.ds_ren_ofi_comercial_niv_7 ,
	offi.ds_ren_ofi_comercial_niv_8 ,
	offi.ds_ren_ofi_comercial_niv_9 ,
	offi.ds_ren_ofi_comercial_niv_10 ,
	offi.ds_ren_ofi_comercial_niv_11
FROM blce_result_ent RE
LEFT JOIN bi_corp_common.dim_ren_oficinas offi ON
	RE.cod_ofi_comercial = offi.cod_ren_ofi_comercial 
DISTRIBUTE BY RE.cod_ofi_comercial 
SORT BY RE.cod_ofi_comercial;

-------------------------------------------------------------------
------------ DROP TEMP --------------------------------------------
-------------------------------------------------------------------
DROP TABLE IF EXISTS blce_result_ent ;

---------------------------------------------------------------------------------
------------ RESULT JOIN GES ----------------------------------------------------
---------------------------------------------------------------------------------
CREATE TEMPORARY TABLE blce_result_ges AS
SELECT /*+ MAPJOIN(bi_corp_staging.rio157_ms0_dm_dwh_gestor_) */
	RE.* ,
	TRIM(GEST.cod_gestor_niv_1) AS cod_ren_gestor_niv_1 ,
	TRIM(GEST.cod_gestor_niv_2) AS cod_ren_gestor_niv_2 ,
	TRIM(GEST.cod_gestor_niv_3) AS cod_ren_gestor_niv_3 ,
	TRIM(GEST.cod_gestor_niv_4) AS cod_ren_gestor_niv_4 ,
	TRIM(GEST.cod_gestor_niv_5) AS cod_ren_gestor_niv_5 ,
	TRIM(GEST.cod_gestor_niv_6) AS cod_ren_gestor_niv_6 ,
	TRIM(GEST.cod_gestor_niv_7) AS cod_ren_gestor_niv_7 ,
	TRIM(GEST.cod_gestor_niv_8) AS cod_ren_gestor_niv_8 ,
	TRIM(GEST.cod_gestor_niv_9) AS cod_ren_gestor_niv_9 ,
	TRIM(GEST.cod_gestor_niv_10) AS cod_ren_gestor_niv_10 ,
	TRIM(GEST.cod_gestor_niv_11) AS cod_ren_gestor_niv_11 ,
	TRIM(GEST.des_gestor_niv_1) AS ds_ren_gestor_niv_1 ,
	TRIM(GEST.des_gestor_niv_2) AS ds_ren_gestor_niv_2 ,
	TRIM(GEST.des_gestor_niv_3) AS ds_ren_gestor_niv_3 ,
	TRIM(GEST.des_gestor_niv_4) AS ds_ren_gestor_niv_4 ,
	TRIM(GEST.des_gestor_niv_5) AS ds_ren_gestor_niv_5 ,
	TRIM(GEST.des_gestor_niv_6) AS ds_ren_gestor_niv_6 ,
	TRIM(GEST.des_gestor_niv_7) AS ds_ren_gestor_niv_7 ,
	TRIM(GEST.des_gestor_niv_8) AS ds_ren_gestor_niv_8 ,
	TRIM(GEST.des_gestor_niv_9) AS ds_ren_gestor_niv_9 ,
	TRIM(GEST.des_gestor_niv_10) AS ds_ren_gestor_niv_10 ,
	TRIM(GEST.des_gestor_niv_11) AS ds_ren_gestor_niv_11
FROM blce_result_offi RE
LEFT JOIN bi_corp_staging.rio157_ms0_dm_dwh_gestor_ gest ON
	RE.cod_gestor_prod = gest.cod_gestor
	AND gest.partition_date = '2020-07-01' 
DISTRIBUTE BY RE.cod_gestor_prod 
SORT BY RE.cod_gestor_prod;

-------------------------------------------------------------------
------------ DROP TEMP --------------------------------------------
-------------------------------------------------------------------
DROP TABLE IF EXISTS blce_result_offi ;

---------------------------------------------------------------------------------
------------ RESULT JOIN ENTJ ---------------------------------------------------
---------------------------------------------------------------------------------
-- CREATE TEMPORARY TABLE blce_result_entj AS
-- SELECT /*+ MAPJOIN(bi_corp_staging.rio157_ms0_dm_je_dwh_entidades_ctr) */
-- 	RE.* ,
-- 	entj.cod_hijo AS cod_ren_entidad_hijo
-- FROM blce_result_ges RE
-- LEFT JOIN bi_corp_staging.rio157_ms0_dm_je_dwh_entidades_ctr entj ON
-- 	RE.cod_pais = entj.cod_pais
-- 	AND entj.partition_date = '2020-07-14' 
-- DISTRIBUTE BY RE.idf_cto_ods ,RE.cod_contenido 
-- SORT BY RE.idf_cto_ods ,RE.cod_contenido;

-------------------------------------------------------------------
------------ DROP TEMP --------------------------------------------
-------------------------------------------------------------------
-- DROP TABLE IF EXISTS blce_result_ges ;


-------------------------------------------------------------------
------------ INSERT TABLE -----------------------------------------
-------------------------------------------------------------------
INSERT OVERWRITE TABLE bi_corp_common.bt_ren_blce_result 
PARTITION(partition_date) 
SELECT
	RE.idf_cto_ods AS cod_ren_contrato,
	RE.fec_alta_cto AS dt_ren_fec_alta_cto,
	RE.fec_ven AS dt_ren_fec_vto_cto,
	RE.fec_reestruc AS dt_ren_fec_reestruc_cto,
	RE.cod_producto AS cod_ren_producto_generic,
	RE.cod_subprodu AS cod_ren_subproducto_generic,
	RE.cod_contenido AS cod_ren_contenido,
	RE.fec_data AS dt_ren_fec_data,
	RE.cod_pais AS cod_ren_pais,
	RE.per_nup AS cod_ren_nup,
	RE.cod_vincula  AS cod_ren_vincula,
	RE.cod_tip_persona  AS cod_ren_tipo_persona,
	RE.nombre_cliente AS ds_ren_nombre_cliente,
	RE.cod_entidad_espana AS cod_ren_entidad_espana,
	RE.cod_producto_gest AS cod_ren_producto_gest,
	RE.cod_divisa AS cod_ren_divisa,
	RE.cod_reajuste AS cod_ren_reajuste,
	RE.cod_agrp_func AS cod_ren_agrp_func,
	RE.cod_est_sdo AS cod_ren_est_sdo,
	RE.cod_cta_cont_gestion AS cod_ren_cont_gestion,
	RE.cod_area_negocio AS cod_ren_area_negocio,
	RE.cod_ren_division,
	RE.ds_ren_division,
	RE.cod_tip_ajuste AS cod_ren_tipo_ajuste,
	RE.cod_centro_cont AS cod_ren_centro_cont,
	RE.cod_ofi_comercial AS cod_ren_ofi_comercial,
	RE.ind_conciliacion AS cod_ren_conciliacion,
	RE.flag_ren_ind_pool,
	RE.cod_segmento_gest AS cod_ren_segmento_gest,
	RE.cod_gestor AS cod_ren_gestor,
	RE.cod_univ AS cod_ren_univ,
	RE.cod_gestor_prod AS cod_ren_gestor_prod,
	RE.cod_origen_inf AS cod_ren_origen_inf,
	null AS cod_ren_entidad_hijo,
	RE.cod_ren_producto_niv_1,
	RE.cod_ren_producto_niv_2,
	RE.cod_ren_producto_niv_3,
	RE.cod_ren_producto_niv_4,
	RE.cod_ren_producto_niv_5,
	RE.cod_ren_producto_niv_6,
	RE.cod_ren_producto_niv_7,
	RE.cod_ren_producto_niv_8,
	RE.cod_ren_producto_niv_9,
	RE.cod_ren_producto_niv_10,
	RE.ds_ren_producto_niv_1,
	RE.ds_ren_producto_niv_2,
	RE.ds_ren_producto_niv_3,
	RE.ds_ren_producto_niv_4,
	RE.ds_ren_producto_niv_5,
	RE.ds_ren_producto_niv_6,
	RE.ds_ren_producto_niv_7,
	RE.ds_ren_producto_niv_8,
	RE.ds_ren_producto_niv_9,
	RE.ds_ren_producto_niv_10,
	RE.cod_ren_cta_resultados_niv_1,
	RE.cod_ren_cta_resultados_niv_2,
	RE.cod_ren_cta_resultados_niv_3,
	RE.cod_ren_cta_resultados_niv_4,
	RE.cod_ren_cta_resultados_niv_5,
	RE.cod_ren_cta_resultados_niv_6,
	RE.cod_ren_cta_resultados_niv_7,
	RE.cod_ren_cta_resultados_niv_8,
	RE.cod_ren_cta_resultados_niv_9,
	RE.cod_ren_cta_resultados_niv_10,
	RE.cod_ren_cta_resultados_niv_11,
	RE.cod_ren_cta_resultados_niv_12,
	RE.ds_ren_cta_resultados_niv_1,
	RE.ds_ren_cta_resultados_niv_2,
	RE.ds_ren_cta_resultados_niv_3,
	RE.ds_ren_cta_resultados_niv_4,
	RE.ds_ren_cta_resultados_niv_5,
	RE.ds_ren_cta_resultados_niv_6,
	RE.ds_ren_cta_resultados_niv_7,
	RE.ds_ren_cta_resultados_niv_8,
	RE.ds_ren_cta_resultados_niv_9,
	RE.ds_ren_cta_resultados_niv_10,
	RE.ds_ren_cta_resultados_niv_11,
	RE.ds_ren_cta_resultados_niv_12,
	RE.cod_ren_area_negocio_niv_1,
	RE.cod_ren_area_negocio_niv_2,
	RE.cod_ren_area_negocio_niv_3,
	RE.cod_ren_area_negocio_niv_4,
	RE.cod_ren_area_negocio_niv_5,
	RE.cod_ren_area_negocio_niv_6,
	RE.cod_ren_area_negocio_niv_7,
	RE.ds_ren_area_negocio_niv_1,
	RE.ds_ren_area_negocio_niv_2,
	RE.ds_ren_area_negocio_niv_3,
	RE.ds_ren_area_negocio_niv_4,
	RE.ds_ren_area_negocio_niv_5,
	RE.ds_ren_area_negocio_niv_6,
	RE.ds_ren_area_negocio_niv_7,
	RE.cod_ren_entidad_gest_niv_1,
	RE.cod_ren_entidad_gest_niv_2,
	RE.cod_ren_entidad_gest_niv_3,
	RE.cod_ren_entidad_gest_niv_4,
	RE.cod_ren_entidad_gest_niv_5,
	RE.cod_ren_entidad_gest_niv_6,
	RE.ds_ren_entidad_gest_niv_1,
	RE.ds_ren_entidad_gest_niv_2,
	RE.ds_ren_entidad_gest_niv_3,
	RE.ds_ren_entidad_gest_niv_4,
	RE.ds_ren_entidad_gest_niv_5,
	RE.ds_ren_entidad_gest_niv_6,
	RE.cod_ren_ofi_comercial_niv_5,
	RE.cod_ren_ofi_comercial_niv_6,
	RE.cod_ren_ofi_comercial_niv_7,
	RE.cod_ren_ofi_comercial_niv_8,
	RE.cod_ren_ofi_comercial_niv_9,
	RE.cod_ren_ofi_comercial_niv_10,
	RE.cod_ren_ofi_comercial_niv_11,
	RE.ds_ren_ofi_comercial_niv_5,
	RE.ds_ren_ofi_comercial_niv_6,
	RE.ds_ren_ofi_comercial_niv_7,
	RE.ds_ren_ofi_comercial_niv_8,
	RE.ds_ren_ofi_comercial_niv_9,
	RE.ds_ren_ofi_comercial_niv_10,
	RE.ds_ren_ofi_comercial_niv_11,
	RE.cod_ren_gestor_niv_1,
	RE.cod_ren_gestor_niv_2,
	RE.cod_ren_gestor_niv_3,
	RE.cod_ren_gestor_niv_4,
	RE.cod_ren_gestor_niv_5,
	RE.cod_ren_gestor_niv_6,
	RE.cod_ren_gestor_niv_7,
	RE.cod_ren_gestor_niv_8,
	RE.cod_ren_gestor_niv_9,
	RE.cod_ren_gestor_niv_10,
	RE.cod_ren_gestor_niv_11,
	RE.ds_ren_gestor_niv_1,
	RE.ds_ren_gestor_niv_2,
	RE.ds_ren_gestor_niv_3,
	RE.ds_ren_gestor_niv_4,
	RE.ds_ren_gestor_niv_5,
	RE.ds_ren_gestor_niv_6,
	RE.ds_ren_gestor_niv_7,
	RE.ds_ren_gestor_niv_8,
	RE.ds_ren_gestor_niv_9,
	RE.ds_ren_gestor_niv_10,
	RE.ds_ren_gestor_niv_11,
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
	RE.fc_ren_saldo_puntual_MO 
FROM blce_result_ges RE
DISTRIBUTE BY RE.idf_cto_ods ,RE.cod_contenido 
SORT BY RE.idf_cto_ods ,RE.cod_contenido;

