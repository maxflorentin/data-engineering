set mapred.job.queue.name=root.dataeng;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.auto.convert.join=true;
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
WHERE partition_date = '2020-06-30' ;

-------------------------------------------------------------------
------------ CLIENTE RESULT ---------------------------------------
-------------------------------------------------------------------
CREATE TEMPORARY TABLE cliente_result AS
SELECT DISTINCT TRIM(idf_pers_ods) AS idf_pers_ods
	, TRIM(cod_vincula) AS cod_vincula
	, TRIM(cod_tip_persona) AS cod_tip_persona
	, CONCAT(nom_nombre,' ', nom_apellido_1) nombre_cliente
FROM bi_corp_staging.rio157_ms0_dt_dwh_cliente_result
WHERE partition_date = '2020-06-30' ;

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
SORT BY RE.idf_cto_ods ,RE.cod_contenido ;
 
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
SORT BY RE.idf_cto_ods ,RE.cod_contenido ;

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
DISTRIBUTE BY RE.idf_cto_ods ,RE.cod_contenido 
SORT BY RE.idf_cto_ods ,RE.cod_contenido ;

-------------------------------------------------------------------
------------ DROP TEMP --------------------------------------------
-------------------------------------------------------------------
DROP TABLE IF EXISTS generic_result ;
DROP TABLE IF EXISTS cliente_result ;
DROP TABLE IF EXISTS blce_result ;
DROP TABLE IF EXISTS blce_result_g ;

-- Solo descripción
---------------------------------------------------------------------------------
------------ RESULT JOIN ADN ----------------------------------------------------
---------------------------------------------------------------------------------
CREATE TEMPORARY TABLE blce_result_adn AS
SELECT 
	RE.* ,
	adn.ds_ren_area_negocio_niv_9 AS ds_ren_division
FROM blce_result_gc RE
LEFT JOIN bi_corp_common.dim_ren_area_de_negocio_ctr adn ON
	RE.cod_area_negocio = adn.cod_ren_area_negocio 
DISTRIBUTE BY RE.idf_cto_ods ,RE.cod_contenido 
SORT BY RE.idf_cto_ods ,RE.cod_contenido ;

-------------------------------------------------------------------
------------ DROP TEMP --------------------------------------------
-------------------------------------------------------------------
DROP TABLE IF EXISTS blce_result_gc ;

-------------------------------------------------------------------
------------ INSERT TABLE -----------------------------------------
-------------------------------------------------------------------
--INSERT OVERWRITE TABLE bi_corp_common.bt_ren_blce_result 
CREATE TEMPORARY TABLE blce_result_rorac AS
--PARTITION(partition_date) 
SELECT DISTINCT 
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
FROM blce_result_adn RE
DISTRIBUTE BY RE.idf_cto_ods ,RE.cod_contenido 
SORT BY RE.idf_cto_ods ,RE.cod_contenido ;

-- DESCRIBE blce_result_rorac

INSERT OVERWRITE TABLE bi_corp_common.bt_ren_blce_result_rorac 
PARTITION(partition_date = '2020-06-30') 
SELECT * FROM blce_result_rorac ;
-- 429048588
SELECT COUNT(1) FROM bi_corp_common.bt_ren_blce_result_rorac

SELECT * FROM bi_corp_common.bt_ren_blce_result_rorac LIMIT 10;

CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_ren_blce_result_rorac (
	cod_ren_contrato	string,
	dt_ren_fec_alta_cto	string,
	dt_ren_fec_vto_cto	string,
	dt_ren_fec_reestruc_cto	string,
	cod_ren_producto_generic	string,
	cod_ren_subproducto_generic	string,
	cod_ren_contenido	string,
	dt_ren_fec_data	string,
	cod_ren_pais	string,
	cod_ren_nup	int,
	cod_ren_vincula	string,
	cod_ren_tipo_persona	string,
	ds_ren_nombre_cliente	string,
	cod_ren_entidad_espana	string,
	cod_ren_producto_gest	string,
	cod_ren_divisa	string,
	cod_ren_reajuste	string,
	cod_ren_agrp_func	string,
	cod_ren_est_sdo	string,
	cod_ren_cont_gestion	string,
	cod_ren_area_negocio	string,
	ds_ren_division	string,
	cod_ren_tipo_ajuste	string,
	cod_ren_centro_cont	string,
	cod_ren_ofi_comercial	string,
	cod_ren_conciliacion	string,
	flag_ren_ind_pool	int,
	cod_ren_segmento_gest	string,
	cod_ren_gestor	string,
	cod_ren_univ	string,
	cod_ren_gestor_prod	string,
	cod_ren_origen_inf	string,
	fc_ren_resultado_saldo_ficticio	double,
	fc_ren_imp_ing_per_ml	double,
	fc_ren_imp_ing_cap_ml	double,
	fc_ren_ing_reaj_ml	double,
	fc_ren_egr_per_ml	double,
	fc_ren_egr_cap_ml	double,
	fc_ren_egr_reaj_ml	double,
	fc_ren_ajtti_egr_tb_cap_ml	double,
	fc_ren_ajtti_egr_sl_cap_ml	double,
	fc_ren_ajtti_egr_per_ml	double,
	fc_ren_ajtti_egr_reajuste_ml	double,
	fc_ren_ajtti_ing_tb_cap_ml	double,
	fc_ren_ajtti_ing_sl_cap_ml	double,
	fc_ren_ajtti_ing_per_ml	double,
	fc_ren_ajtti_ing_reajuste_ml	double,
	fc_ren_efec_enc_ml	double,
	fc_ren_ing_per_mo	double,
	fc_ren_ing_cap_mo	double,
	fc_ren_ing_reaj_mo	double,
	fc_ren_egr_per_mo	double,
	fc_ren_egr_cap_mo	double,
	fc_ren_egr_reaj_mo	double,
	fc_ren_ajtti_egr_tb_cap_mo	double,
	fc_ren_ajtti_egr_sl_cap_mo	double,
	fc_ren_ajtti_egr_per_mo	double,
	fc_ren_ajtti_egr_reajuste_mo	double,
	fc_ren_ajtti_ing_tb_cap_mo	double,
	fc_ren_ajtti_ing_sl_cap_mo	double,
	fc_ren_ajtti_ing_per_mo	double,
	fc_ren_ajtti_ing_reajuste_mo	double,
	fc_ren_efec_enc_mo	double,
	fc_ren_sdo_cap_med_ml	double,
	fc_ren_sdo_med_int_ml	double,
	fc_ren_sdo_med_reajuste_ml	double,
	fc_ren_sdo_med_insolv_ml	double,
	fc_ren_sdo_cap_med_mo	double,
	fc_ren_sdo_med_int_mo	double,
	fc_ren_sdo_med_reajuste_mo	double,
	fc_ren_sdo_med_insolv_mo	double,
	fc_ren_ing_per_ml	double,
	fc_ren_restultado_total_ml	double,
	fc_ren_restultado_total_mo	double,
	fc_ren_saldo_medio_total_ml	double,
	fc_ren_saldo_medio_total_mo	double,
	fc_ren_resultado_total_real_ml	double,
	fc_ren_resultado_total_real_mo	double,
	fc_ren_saldo_puntual_ml	double,
	fc_ren_saldo_puntual_mo	double)
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/santander/bi-corp/common/rentabilidad/fact/bt_ren_blce_result_rorac'

