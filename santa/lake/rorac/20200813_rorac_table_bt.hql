set mapred.job.queue.name=root.dataeng;

--   __                             __          __     __           
--  / /_ ___   ____ ___   ____     / /_ ____ _ / /_   / /___   _____
-- / __// _ \ / __ `__ \ / __ \   / __// __ `// __ \ / // _ \ / ___/
--/ /_ /  __// / / / / // /_/ /  / /_ / /_/ // /_/ // //  __/(__  ) 
--\__/ \___//_/ /_/ /_// .___/   \__/ \__,_//_.___//_/ \___//____/  
--                    /_/                                           

-- /* GENERIC RESULT */
CREATE TEMPORARY TABLE generic_result AS
-- /* data contratos */
SELECT DISTINCT idf_cto_ods
	, cod_contenido
	, cod_pais
	, from_unixtime(unix_timestamp(fec_alta_cto , 'yyyyMMdd'), 'yyyy-MM-dd') fec_alta_cto
	, from_unixtime(unix_timestamp(fec_ven, 'yyyyMMdd'), 'yyyy-MM-dd') fec_ven
	, from_unixtime(unix_timestamp(fec_reestruc, 'yyyyMMdd'), 'yyyy-MM-dd') fec_reestruc
	, IF(cod_producto = 'null', null, cod_producto) cod_producto
	, IF(cod_subprodu = 'null', null, cod_subprodu) cod_subprodu
FROM bi_corp_staging.rio157_ms0_dt_dwh_generic_result 
WHERE partition_date = '2020-06-30' 
SORT BY idf_cto_ods	, cod_contenido ;

-- /* CLIENTE RESULT */
CREATE TEMPORARY TABLE cliente_result AS
-- /* data clientes */
SELECT DISTINCT idf_pers_ods
	, CAST(SUBSTR(idf_pers_ods, 6, 8) AS INT) per_nup
	, cod_vincula
	, cod_tip_persona
	, CONCAT(IF(nom_nombre = 'null', '', nom_nombre),' ',IF(nom_apellido_1 = 'null', '', nom_apellido_1)) nombre_cliente
FROM bi_corp_staging.rio157_ms0_dt_dwh_cliente_result
WHERE partition_date = '2020-06-30' 
SORT BY idf_pers_ods ;

-- /* BLCE_RESULT */
CREATE TEMPORARY TABLE blce_result AS 
-- /* data blce_result con saldos != 0 */
SELECT 
	CONCAT(RE.cod_contenido,RE.idf_cto_ods) AS cod_ren_contrato_rorac ,
	RE.idf_cto_ods ,
	RE.cod_contenido ,
	from_unixtime(unix_timestamp(RE.fec_data , 'yyyyMMdd'), 'yyyy-MM-dd') fec_data ,
	RE.cod_pais ,
	RE.idf_pers_ods ,
	CAST(SUBSTRING(RE.idf_pers_ods,6,8) AS INT) AS per_nup ,
	RE.cod_entidad_espana ,
	RE.cod_producto_gest ,
	RE.cod_divisa ,
	IF(RE.cod_reajuste = 'null', null, RE.cod_reajuste) cod_reajuste ,
	RE.cod_agrp_func ,
	RE.cod_est_sdo ,
	RE.cod_cta_cont_gestion ,
	RE.cod_area_negocio ,
	RE.cod_tip_ajuste ,
	RE.cod_centro_cont ,
	RE.cod_ofi_comercial ,
	RE.ind_conciliacion ,
	CASE WHEN TRIM(RE.ind_pool) = 'S' THEN 1 ELSE 0 END flag_ren_ind_pool ,
	RE.cod_segmento_gest ,
	RE.cod_gestor ,
	RE.cod_univ ,
	IF(RE.cod_gestor_prod = 'null', null, RE.cod_gestor_prod) cod_gestor_prod ,
	RE.cod_origen_inf , 
	sum(COALESCE(RE.OUT_TTI, 0)) AS fc_ren_resultado_saldo_ficticio , -- cambiar nombre out_tti --
	sum(COALESCE(RE.IMP_ING_PER_ML, 0)) AS fc_ren_imp_ing_per_ml ,
	sum(COALESCE(RE.IMP_ING_CAP_ML, 0)) AS fc_ren_imp_ing_cap_ml ,
	sum(COALESCE(RE.IMP_ING_REAJ_ML, 0)) AS fc_ren_ing_reaj_ml ,
	sum(COALESCE(RE.IMP_EGR_PER_ML, 0)) AS fc_ren_egr_per_ml ,
	sum(COALESCE(RE.IMP_EGR_CAP_ML, 0)) AS fc_ren_egr_cap_ml ,
	sum(COALESCE(RE.IMP_EGR_REAJ_ML, 0)) AS fc_ren_egr_reaj_ml ,
	sum(COALESCE(RE.IMP_AJTTI_EGR_TB_CAP_ML, 0)) AS fc_ren_ajtti_egr_tb_cap_ml ,
	sum(COALESCE(RE.IMP_AJTTI_EGR_SL_CAP_ML, 0)) AS fc_ren_ajtti_egr_sl_cap_ml ,
	sum(COALESCE(RE.IMP_AJTTI_EGR_PER_ML, 0)) AS fc_ren_ajtti_egr_per_ml ,
	sum(COALESCE(RE.IMP_AJTTI_EGR_REAJUSTE_ML, 0)) AS fc_ren_ajtti_egr_reajuste_ml ,
	sum(COALESCE(RE.IMP_AJTTI_ING_TB_CAP_ML, 0)) AS fc_ren_ajtti_ing_tb_cap_ml ,
	sum(COALESCE(RE.IMP_AJTTI_ING_SL_CAP_ML, 0)) AS fc_ren_ajtti_ing_sl_cap_ml ,
	sum(COALESCE(RE.IMP_AJTTI_ING_PER_ML, 0)) AS fc_ren_ajtti_ing_per_ml ,
	sum(COALESCE(RE.IMP_AJTTI_ING_REAJUSTE_ML, 0)) AS fc_ren_ajtti_ing_reajuste_ml ,
	sum(COALESCE(RE.IMP_EFEC_ENC_ML, 0)) AS fc_ren_efec_enc_ml ,
	sum(COALESCE(RE.IMP_ING_PER_MO, 0)) AS fc_ren_ing_per_mo ,
	sum(COALESCE(RE.IMP_ING_CAP_MO, 0)) AS fc_ren_ing_cap_mo ,
	sum(COALESCE(RE.IMP_ING_REAJ_MO, 0)) AS fc_ren_ing_reaj_mo ,
	sum(COALESCE(RE.IMP_EGR_PER_MO, 0)) AS fc_ren_egr_per_mo ,
	sum(COALESCE(RE.IMP_EGR_CAP_MO, 0)) AS fc_ren_egr_cap_mo ,
	sum(COALESCE(RE.IMP_EGR_REAJ_MO, 0)) AS fc_ren_egr_reaj_mo ,
	sum(COALESCE(RE.IMP_AJTTI_EGR_TB_CAP_MO, 0)) AS fc_ren_ajtti_egr_tb_cap_mo ,
	sum(COALESCE(RE.IMP_AJTTI_EGR_SL_CAP_MO, 0)) AS fc_ren_ajtti_egr_sl_cap_mo ,
	sum(COALESCE(RE.IMP_AJTTI_EGR_PER_MO, 0)) AS fc_ren_ajtti_egr_per_mo ,
	sum(COALESCE(RE.IMP_AJTTI_EGR_REAJUSTE_MO, 0)) AS fc_ren_ajtti_egr_reajuste_mo ,
	sum(COALESCE(RE.IMP_AJTTI_ING_TB_CAP_MO, 0)) AS fc_ren_ajtti_ing_tb_cap_mo ,
	sum(COALESCE(RE.IMP_AJTTI_ING_SL_CAP_MO, 0)) AS fc_ren_ajtti_ing_sl_cap_mo ,
	sum(COALESCE(RE.IMP_AJTTI_ING_PER_MO, 0)) AS fc_ren_ajtti_ing_per_mo ,
	sum(COALESCE(RE.IMP_AJTTI_ING_REAJUSTE_MO, 0)) AS fc_ren_ajtti_ing_reajuste_mo ,
	sum(COALESCE(RE.IMP_EFEC_ENC_MO, 0)) AS fc_ren_efec_enc_mo ,
	sum(COALESCE(RE.IMP_SDO_CAP_MED_ML, 0)) AS fc_ren_sdo_cap_med_ml ,
	sum(COALESCE(RE.IMP_SDO_MED_INT_ML, 0)) AS fc_ren_sdo_med_int_ml ,
	sum(COALESCE(RE.IMP_SDO_MED_REAJUSTE_ML, 0)) AS fc_ren_sdo_med_reajuste_ml ,
	sum(COALESCE(RE.IMP_SDO_MED_INSOLV_ML, 0)) AS fc_ren_sdo_med_insolv_ml ,
	sum(COALESCE(RE.IMP_SDO_CAP_MED_MO, 0)) AS fc_ren_sdo_cap_med_mo ,
	sum(COALESCE(RE.IMP_SDO_MED_INT_MO, 0)) AS fc_ren_sdo_med_int_mo ,
	sum(COALESCE(RE.IMP_SDO_MED_REAJUSTE_MO, 0)) AS fc_ren_sdo_med_reajuste_mo ,
	sum(COALESCE(RE.IMP_SDO_MED_INSOLV_MO, 0)) AS fc_ren_sdo_med_insolv_mo ,
	sum(COALESCE(RE.IMP_ING_PER_ML, 0)) AS fc_ren_ing_per_ml ,
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
	+ sum(COALESCE(RE.IMP_EFEC_ENC_ML, 0))) AS fc_ren_restultado_total_ML , 
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
	+ sum(COALESCE(RE.IMP_EFEC_ENC_MO, 0))) AS fc_ren_restultado_total_MO , 
	(sum(COALESCE(RE.IMP_SDO_CAP_MED_ML, 0)) 
	+ sum(COALESCE(RE.IMP_SDO_MED_INT_ML, 0)) 
	+ sum(COALESCE(RE.IMP_SDO_MED_REAJUSTE_ML, 0)) 
	+ sum(COALESCE(RE.IMP_SDO_MED_INSOLV_ML, 0))) AS fc_ren_saldo_medio_total_ML , 
	(sum(COALESCE(RE.IMP_SDO_CAP_MED_MO, 0)) 
	+ sum(COALESCE(RE.IMP_SDO_MED_INT_MO, 0)) 
	+ sum(COALESCE(RE.IMP_SDO_MED_REAJUSTE_MO, 0)) 
	+ sum(COALESCE(RE.IMP_SDO_MED_INSOLV_MO, 0))) AS fc_ren_saldo_medio_total_MO , 
	(sum(COALESCE(RE.IMP_ING_PER_ML, 0)) 
	+ sum(COALESCE(RE.IMP_ING_CAP_ML, 0)) 
	+ sum(COALESCE(RE.IMP_ING_REAJ_ML, 0)) 
	+ sum(COALESCE(RE.IMP_EGR_PER_ML, 0)) 
	+ sum(COALESCE(RE.IMP_EGR_CAP_ML, 0)) 
	+ sum(COALESCE(RE.IMP_EGR_REAJ_ML, 0))) AS fc_ren_resultado_total_real_ML ,
	(sum(COALESCE(RE.IMP_ING_PER_MO, 0)) 
	+ sum(COALESCE(RE.IMP_ING_CAP_MO, 0)) 
	+ sum(COALESCE(RE.IMP_ING_REAJ_MO, 0)) 
	+ sum(COALESCE(RE.IMP_EGR_PER_MO, 0)) 
	+ sum(COALESCE(RE.IMP_EGR_CAP_MO, 0)) 
	+ sum(COALESCE(RE.IMP_EGR_REAJ_MO, 0))) AS fc_ren_resultado_total_real_MO ,
	(sum(COALESCE(RE.IMP_SDO_CAP_ML, 0)) 
	+ sum(COALESCE(RE.IMP_SDO_INT_ML, 0)) 
	+ sum(COALESCE(RE.IMP_SDO_INSOLV_ML, 0)) 
	+ sum(COALESCE(RE.IMP_SDO_REAJUSTE_ML, 0))) AS fc_ren_saldo_puntual_ML ,
	(sum(COALESCE(RE.IMP_SDO_CAP_MO, 0)) 
	+ sum(COALESCE(RE.IMP_SDO_INT_MO, 0)) 
	+ sum(COALESCE(RE.IMP_SDO_INSOLV_MO, 0)) 
	+ sum(COALESCE(RE.IMP_SDO_REAJUSTE_MO, 0))) AS fc_ren_saldo_puntual_MO ,
	RE.partition_date 
FROM bi_corp_staging.rio157_ms0_ft_dwh_blce_result RE 
WHERE RE.partition_date = '2020-06-30' 
AND (RE.imp_sdo_cap_ml	<> '0'
    or RE.imp_sdo_cap_mo	<> '0'
    or RE.imp_sdo_int_ml	<> '0'
    or RE.imp_sdo_int_mo	<> '0'
    or RE.imp_sdo_reajuste_ml <> '0'
    or RE.imp_sdo_reajuste_mo <> '0'
    or RE.imp_sdo_insolv_ml	<> '0'
    or RE.imp_sdo_insolv_mo	<> '0'
    or RE.imp_sdo_cap_med_ml	<> '0'
    or RE.imp_sdo_cap_med_mo	<> '0'
    or RE.imp_sdo_med_int_ml	<> '0'
    or RE.imp_sdo_med_int_mo	<> '0'
    or RE.imp_sdo_med_reajuste_ml <> '0'
    or RE.imp_sdo_med_reajuste_mo <> '0'
    or RE.imp_sdo_med_insolv_ml	<> '0'
    or RE.imp_sdo_med_insolv_mo	<> '0'
    or RE.imp_egr_cap_ml	<> '0'
    or RE.imp_egr_cap_mo	<> '0'
    or RE.imp_egr_per_ml	<> '0'
    or RE.imp_egr_per_mo	<> '0'
    or RE.imp_egr_reaj_ml <> '0'
    or RE.imp_egr_reaj_mo <> '0'
    or RE.imp_ing_cap_ml	<> '0'
    or RE.imp_ing_cap_mo	<> '0'
    or RE.imp_ing_per_ml	<> '0'
    or RE.imp_ing_per_mo	<> '0'
    or RE.imp_ing_reaj_ml <> '0'
    or RE.imp_ing_reaj_mo <> '0'
    or RE.imp_ajtti_egr_tb_cap_ml <> '0'
    or RE.imp_ajtti_egr_tb_cap_mo <> '0'
    or RE.imp_ajtti_egr_sl_cap_ml <> '0'
    or RE.imp_ajtti_egr_sl_cap_mo <> '0'
    or RE.imp_ajtti_egr_per_ml <> '0'
    or RE.imp_ajtti_egr_per_mo <> '0'
    or RE.imp_ajtti_egr_reajuste_ml	<> '0'
    or RE.imp_ajtti_egr_reajuste_mo	<> '0'
    or RE.imp_ajtti_ing_tb_cap_ml <> '0'
    or RE.imp_ajtti_ing_tb_cap_mo <> '0'
    or RE.imp_ajtti_ing_sl_cap_ml <> '0'
    or RE.imp_ajtti_ing_sl_cap_mo <> '0'
    or RE.imp_ajtti_ing_per_ml	<> '0'
    or RE.imp_ajtti_ing_per_mo	<> '0'
    or RE.imp_ajtti_ing_reajuste_ml	<> '0'
    or RE.imp_ajtti_ing_reajuste_mo	<> '0'
    or RE.imp_efec_enc_ml <> '0'
    or RE.imp_efec_enc_mo <> '0')
GROUP BY CONCAT(RE.cod_contenido,RE.idf_cto_ods) ,
	RE.idf_cto_ods ,
	RE.cod_contenido ,
	RE.fec_data ,
	RE.cod_pais ,
	RE.idf_pers_ods ,
	CAST(SUBSTRING(RE.idf_pers_ods,6,8) AS INT) ,
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
	RE.ind_pool ,
	RE.cod_segmento_gest ,
	RE.cod_gestor ,
	RE.cod_univ ,
	RE.cod_gestor_prod ,
	RE.cod_origen_inf ,
	RE.partition_date ;
 
-- /* RESULT G */
CREATE TEMPORARY TABLE blce_result_g AS
-- /* data blce_result + generic */
SELECT RE.*
	, gen.cod_producto
	, gen.cod_subprodu
	, gen.fec_alta_cto
	, gen.fec_ven
	, gen.fec_reestruc
FROM blce_result RE
LEFT JOIN generic_result gen ON 
	RE.idf_cto_ods = gen.idf_cto_ods
	AND RE.cod_contenido = gen.cod_contenido ;

-- /* RESULT GC */
CREATE TEMPORARY TABLE blce_result_gc AS
-- /* data blce_result + generic + clientes */
SELECT RE.*	
	, cli.cod_vincula
	, cli.cod_tip_persona
	, cli.nombre_cliente
FROM blce_result_g RE
LEFT JOIN cliente_result cli ON
	cli.idf_pers_ods = RE.idf_pers_ods ;

-- /* DROP TEMP */
DROP TABLE IF EXISTS generic_result ;
DROP TABLE IF EXISTS cliente_result ;
DROP TABLE IF EXISTS blce_result ;
DROP TABLE IF EXISTS blce_result_g ;


--    _                       __                  __   _                        ___
--   (_)____   ____   __  __ / /_   ____ _ _____ / /_ (_)_   __ ____     _   __<  /
--  / // __ \ / __ \ / / / // __/  / __ `// ___// __// /| | / // __ \   | | / // / 
-- / // / / // /_/ // /_/ // /_   / /_/ // /__ / /_ / / | |/ // /_/ /   | |/ // /  
--/_//_/ /_// .___/ \__,_/ \__/   \__,_/ \___/ \__//_/  |___/ \____/    |___//_/   
--         /_/    

CREATE TEMPORARY TABLE result_activo AS
-- /* input activo + jerarquias */
SELECT RE.* ,
	adn.cod_ren_area_negocio_niv_9 ,
	adn.ds_ren_area_negocio_niv_9 ,
	prod.cod_ren_producto_niv_3 ,
	prod.cod_ren_producto_niv_4 ,
	cta.cod_ren_cta_resultados_niv_8 ,
	cta.cod_ren_cta_resultados_niv_9 
FROM blce_result_gc RE
JOIN bi_corp_common.dim_ren_productos_ctr prod ON
	RE.cod_producto_gest = prod.cod_ren_producto 
	AND prod.cod_ren_producto_niv_3 IN ('BG1','BG3')
JOIN bi_corp_common.dim_ren_area_de_negocio_ctr adn ON 
	RE.cod_area_negocio = adn.cod_ren_area_negocio 
JOIN bi_corp_staging.rio157_ms0_dm_je_dwh_entidades_ctr entj ON 
	entj.partition_date = '2020-07-14'
	AND RE.cod_pais = entj.cod_pais 
	AND entj.cod_hijo = '10001'
JOIN bi_corp_common.dim_ren_cta_resultados_ctr cta ON
	RE.cod_cta_cont_gestion  = cta.cod_ren_cta_resultados
WHERE RE.partition_date = '2020-06-30';

------------ /* RESULT ACTIVO I */
CREATE TEMPORARY TABLE result_activo_i AS
-- /* input activo + jerarquias + metricas de los contenidos: 'PRE','CRE','ARF','CCO' */
SELECT 
	'ARG' AS cod_ren_unidad	 ,
	fec_data AS dt_ren_fec_data	 ,
	NULL AS cod_ren_finalidad_datos	 ,
	cod_ren_contrato_rorac ,
	idf_cto_ods AS cod_ren_contrato	 ,
	cod_area_negocio ,
	'ARS' AS  cod_ren_divisa ,
	cod_ren_area_negocio_niv_9 AS cod_ren_division ,
	cod_producto , 
	idf_pers_ods ,
	CASE WHEN cod_ren_producto_niv_3 = 'BG1'THEN SUM(fc_ren_restultado_total_ml) ELSE 0 END AS fc_ren_margenmes ,
	CASE WHEN cod_ren_producto_niv_3 = 'BG1'THEN SUM(fc_ren_saldo_medio_total_ml) ELSE 0 END AS fc_ren_sdbsmv ,
	CASE WHEN cod_ren_producto_niv_3 = 'BG3' THEN (SUM(fc_ren_sdo_cap_med_ml)
										+ SUM(fc_ren_sdo_med_int_ml)
										+ SUM(fc_ren_sdo_med_reajuste_ml)
										+ SUM(fc_ren_sdo_med_insolv_ml)) ELSE 0 END AS fc_ren_sfbsmv , 
	fec_alta_cto	 ,
	fec_ven	 ,
	cod_entidad_espana	 ,
	cod_centro_cont ,
	cod_subprodu ,
    NULL AS zona ,
    NULL AS territorial,
	cod_gestor_prod	 ,
	cod_gestor	 ,
	CASE WHEN cod_ren_producto_niv_3 = 'BG1' THEN (SUM(fc_ren_imp_ing_per_ml)
										+ SUM(fc_ren_imp_ing_cap_ml)
										+ SUM(fc_ren_sdo_cap_med_ml)
										+ SUM(fc_ren_sdo_med_int_ml)
										+ SUM(fc_ren_sdo_med_reajuste_ml)
										+ SUM(fc_ren_sdo_med_insolv_ml)) ELSE 0 END AS fc_ren_sdbsfm ,
	CASE WHEN cod_ren_producto_niv_3 = 'BG1' THEN SUM(fc_ren_resultado_total_real_ml) ELSE 0 END AS fc_ren_interes ,
	CASE WHEN cod_ren_cta_resultados_niv_9 = '1032' THEN SUM(fc_ren_resultado_total_real_ml) ELSE 0 END AS fc_ren_comfin ,
	CASE WHEN cod_ren_cta_resultados_niv_8 = '1051' THEN SUM(fc_ren_resultado_total_real_ml) ELSE 0 END AS fc_ren_comnofin ,
	cod_vincula ,
	CASE WHEN cod_ren_producto_niv_4 = 'LN004' THEN SUM(fc_ren_resultado_total_real_ml) ELSE 0 END AS fc_ren_rof ,
	SUM(fc_ren_resultado_saldo_ficticio) AS fc_ren_tti , -- OUT_TTI blce_result --
    NULL AS  mtm ,
    NULL AS  nocional ,
    cod_divisa ,
    NULL AS flag_mora_local , 
    NULL AS flag_carterizadas ,
	nombre_cliente ,
	cod_tip_persona ,
    NULL AS nifcif , 
    cod_origen_inf AS intragrupo ,
	NULL AS cod_ren_internegocio , -- (resolver desde blce result) -- 
    cod_ren_area_negocio_niv_9 AS area_negocio_corp , 
	cod_producto_gest ,
	cod_segmento_gest ,
    cod_producto AS cod_producto_operacional,
	NULL AS ds_ren_ficheromis ,
	fec_reestruc ,
	partition_date AS dt_ren_fec_extr_datos ,
    NULL AS flagneteo ,
	CASE WHEN cod_ren_cta_resultados_niv_8 = '1054' THEN SUM(fc_ren_resultado_total_real_ml) ELSE 0 END AS fc_ren_orex ,
	NULL AS fc_ren_costemensualcto
FROM result_activo
WHERE partition_date = '2020-06-30'
AND cod_contenido IN ('PRE','CRE','ARF','CCO')
GROUP BY 
		--'ARG' AS cod_ren_unidad	 ,
		fec_data ,--S dt_ren_fec_data	 ,
		--NULL AS cod_ren_finalidad_datos	 ,
		cod_ren_contrato_rorac ,
		idf_cto_ods ,--AS cod_ren_contrato	 ,
		cod_area_negocio ,
		--'ARS' AS  cod_ren_divisa ,
		cod_ren_area_negocio_niv_9 ,--AS cod_ren_division ,
		cod_producto , --blce_result
		idf_pers_ods ,
		fec_alta_cto	 ,
		fec_ven	 ,
		cod_entidad_espana	 ,
		cod_centro_cont ,
		cod_subprodu ,
	    --NULL AS zona ,
	    --NULL AS territorial,
		cod_gestor_prod	 ,
		cod_gestor	 ,
		cod_vincula ,
		--NULL AS  mtm ,
	    --NULL AS  nocional ,
	    cod_divisa ,
	    --NULL AS flag_mora_local , --Flagmoralocal,
	    --NULL AS flag_carterizadas ,--FlagCarterizadas,
		nombre_cliente ,
		cod_tip_persona ,
	    --NULL AS nifcif , --NIFCIF,
	    cod_origen_inf ,--AS intragrupo ,
		--NULL AS cod_ren_internegocio , -- (resolver desde blce result) -- 
	    cod_ren_area_negocio_niv_9 ,--AS area_negocio_corp , 
		cod_producto_gest ,
		cod_segmento_gest ,
	    --cod_producto AS cod_producto_operacional,
		--NULL AS ds_ren_ficheromis ,
		fec_reestruc ,
		partition_date ,--AS dt_ren_fec_extr_datos ,
	    --NULL AS flagneteo ,
		--NULL AS fc_ren_costemensualcto,--AS fc_ren_costemensualcto ,
		cod_ren_producto_niv_3 ,
		cod_ren_producto_niv_4 ,
		cod_ren_cta_resultados_niv_8 , 
		cod_ren_cta_resultados_niv_9

	
-- /* CREATE TABLE COMMON */
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_ren_rorac_input_activo (	
	cod_ren_unidad string,
	dt_ren_data string,
	cod_ren_finalidaddatos string,
	cod_ren_contrato_rorac string,
	cod_ren_contrato string,
	cod_ren_areanegocio string,
	cod_ren_divisa string,
	cod_ren_division string,
	cod_ren_producto string,
	cod_ren_pers_ods string,
	fc_ren_margenmes DECIMAL(20,4),
	fc_ren_sdbsmv DECIMAL(20,4),
	fc_ren_sfbsmv DECIMAL(20,4),
	dt_ren_fec_altacto string,
	dt_ren_fec_ven string,
	cod_ren_entidad_espana string,
	cod_ren_centrocont string,
	cod_ren_subprodu string,
	cod_ren_zona string,
	cod_ren_territorial string,
	cod_ren_gestorprod string,
	cod_ren_gestor string,
	fc_ren_sdbsfm DECIMAL(20,4),
	fc_ren_interes DECIMAL(20,4),
	fc_ren_comfin DECIMAL(20,4),
	fc_ren_comnofin DECIMAL(20,4),
	cod_ren_vincula string,
	fc_ren_rof DECIMAL(20,4),
	fc_ren_tti DECIMAL(20,4),
	cod_ren_mtm string,
	cod_ren_nocional string,
	cod_ren_divisa_mis string,
	flag_ren_moralocal int,
	flag_ren_carterizadas int,
	ds_ren_nombrecliente string,
	cod_ren_tipopersona string,
	cod_ren_nifcif string,
	ds_ren_intragrupo string,
	cod_ren_internegocio string,
	cod_ren_areanegociocorp string,
	cod_productogest string,
	cod_segmentogest string,
	cod_producto_operacional string,
	ds_ren_ficheromis string,
	dt_ren_fec_reestruc string,
	dt_ren_fec_extrdatos string,
	flag_ren_neteo int,
	fc_ren_orex DECIMAL(20,4),
	fc_ren_costemensualcto DECIMAL(20,4)
)
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/santander/bi-corp/common/rentabilidad/fact/bt_ren_rorac_input_activo2'

--/* INSERT ACTIVO */
INSERT OVERWRITE TABLE bi_corp_common.bt_ren_rorac_input_activo 
PARTITION(partition_date='2020-06-30') 
SELECT cod_ren_unidad ,
dt_ren_fec_data ,
cod_ren_finalidad_datos ,
cod_ren_contrato_rorac ,
cod_ren_contrato ,
cod_area_negocio ,
cod_ren_divisa ,
cod_ren_division ,
cod_producto ,
idf_pers_ods ,
fc_ren_margenmes ,
fc_ren_sdbsmv ,
fc_ren_sfbsmv ,
fec_alta_cto ,
fec_ven ,
cod_entidad_espana ,
cod_centro_cont ,
cod_subprodu ,
zona ,
territorial ,
cod_gestor_prod ,
cod_gestor ,
fc_ren_sdbsfm ,
fc_ren_interes ,
fc_ren_comfin ,
fc_ren_comnofin ,
cod_vincula ,
fc_ren_rof ,
fc_ren_tti ,
mtm ,
nocional ,
cod_divisa ,
flag_mora_local ,
flag_carterizadas ,
nombre_cliente ,
cod_tip_persona ,
nifcif ,
intragrupo ,
cod_ren_internegocio ,
area_negocio_corp ,
cod_producto_gest ,
cod_segmento_gest ,
cod_producto_operacional ,
ds_ren_ficheromis ,
fec_reestruc ,
dt_ren_fec_extr_datos ,
flagneteo ,
fc_ren_orex ,
fc_ren_costemensualcto 
FROM result_activo_i2 ;



--    _                       __                         _                        ___
--   (_)____   ____   __  __ / /_   ____   ____ _ _____ (_)_   __ ____     _   __<  /
--  / // __ \ / __ \ / / / // __/  / __ \ / __ `// ___// /| | / // __ \   | | / // / 
-- / // / / // /_/ // /_/ // /_   / /_/ // /_/ /(__  )/ / | |/ // /_/ /   | |/ // /  
--/_//_/ /_// .___/ \__,_/ \__/  / .___/ \__,_//____//_/  |___/ \____/    |___//_/   
--         /_/                  /_/                                                 
CREATE TEMPORARY TABLE result_pasivo AS
-- /* input pasivo + jerarquias */
SELECT RE.* ,
	adn.cod_ren_area_negocio_niv_9 ,
	adn.ds_ren_area_negocio_niv_9 ,
	prod.cod_ren_producto_niv_3 ,
	prod.cod_ren_producto_niv_4 ,
	cta.cod_ren_cta_resultados_niv_8 ,
	cta.cod_ren_cta_resultados_niv_9 
FROM blce_result_gc RE
JOIN bi_corp_common.dim_ren_productos_ctr prod ON
	RE.cod_producto_gest = prod.cod_ren_producto 
	AND prod.cod_ren_producto_niv_3 IN ('BG2','BG3')
JOIN bi_corp_common.dim_ren_area_de_negocio_ctr adn ON 
	RE.cod_area_negocio = adn.cod_ren_area_negocio 
JOIN bi_corp_staging.rio157_ms0_dm_je_dwh_entidades_ctr entj ON 
	entj.partition_date = '2020-07-14'
	AND RE.cod_pais = entj.cod_pais 
	AND entj.cod_hijo = '10001'
JOIN bi_corp_common.dim_ren_cta_resultados_ctr cta ON
	RE.cod_cta_cont_gestion  = cta.cod_ren_cta_resultados
WHERE RE.partition_date = '2020-06-30';

-- /* RESULT PASIVO I */
CREATE TEMPORARY TABLE result_pasivo_i AS
-- /* input activo + jerarquias + metricas de los contenidos: 'PLZ','CTA' */
SELECT 
	'ARG' AS cod_ren_unidad	 ,
	fec_data AS dt_ren_fec_data	 ,
	NULL AS cod_ren_finalidad_datos	 ,
	cod_ren_contrato_rorac ,
	idf_cto_ods AS cod_ren_contrato	 ,
	cod_area_negocio ,
	'ARS' AS  cod_ren_divisa ,
	cod_ren_area_negocio_niv_9 AS cod_ren_division ,
	cod_producto , --blce_result
	idf_pers_ods ,
	CASE WHEN cod_ren_producto_niv_3 = 'BG2' THEN SUM(fc_ren_restultado_total_ml) ELSE 0 END AS fc_ren_margenmes ,
	CASE WHEN cod_ren_producto_niv_3 = 'BG2' THEN SUM(fc_ren_saldo_medio_total_ml) ELSE 0 END AS fc_ren_sdbsmv ,
	CASE WHEN cod_ren_producto_niv_3 = 'BG3' THEN (SUM(fc_ren_sdo_cap_med_ml)
										+ SUM(fc_ren_sdo_med_int_ml)
										+ SUM(fc_ren_sdo_med_reajuste_ml)
										+ SUM(fc_ren_sdo_med_insolv_ml)) ELSE 0 END AS fc_ren_sfbsmv , 
	fec_alta_cto	 ,
	fec_ven	 ,
	cod_entidad_espana	 ,
	cod_centro_cont ,
	cod_subprodu ,
    NULL AS zona ,
    NULL AS territorial,
	cod_gestor_prod	 ,
	cod_gestor	 ,
	CASE WHEN cod_ren_producto_niv_3 = 'BG2' THEN (SUM(fc_ren_imp_ing_per_ml)
										+ SUM(fc_ren_imp_ing_cap_ml)
										+ SUM(fc_ren_sdo_cap_med_ml)
										+ SUM(fc_ren_sdo_med_int_ml)
										+ SUM(fc_ren_sdo_med_reajuste_ml)
										+ SUM(fc_ren_sdo_med_insolv_ml)) ELSE 0 END AS fc_ren_sdbsfm ,
	CASE WHEN cod_ren_producto_niv_3 = 'BG2' THEN SUM(fc_ren_resultado_total_real_ml) ELSE 0 END AS fc_ren_interes ,
	CASE WHEN cod_ren_cta_resultados_niv_9 = '1032' THEN SUM(fc_ren_resultado_total_real_ml) ELSE 0 END AS fc_ren_comfin ,
	CASE WHEN cod_ren_cta_resultados_niv_8 = '1051' THEN SUM(fc_ren_resultado_total_real_ml) ELSE 0 END AS fc_ren_comnofin ,
	cod_vincula ,
	CASE WHEN cod_ren_producto_niv_4 = 'LN004' THEN SUM(fc_ren_resultado_total_real_ml) ELSE 0 END AS fc_ren_rof ,
	SUM(fc_ren_resultado_saldo_ficticio) AS fc_ren_tti , -- OUT_TTI blce_result --
    NULL AS  mtm ,
    NULL AS  nocional ,
    cod_divisa ,
    NULL AS flag_mora_local , --Flagmoralocal,
    NULL AS flag_carterizadas ,--FlagCarterizadas,
	nombre_cliente ,
	cod_tip_persona ,
    NULL AS nifcif , --NIFCIF,
    cod_origen_inf AS intragrupo ,
	NULL AS cod_ren_internegocio , -- (resolver desde blce result) -- 
    cod_ren_area_negocio_niv_9 AS area_negocio_corp , 
	cod_producto_gest ,
	cod_segmento_gest ,
    cod_producto AS cod_producto_operacional,
	NULL AS ds_ren_ficheromis ,
	fec_reestruc ,
	partition_date AS dt_ren_fec_extr_datos ,
    NULL AS flagneteo ,
	CASE WHEN cod_ren_cta_resultados_niv_8 = '1054' THEN SUM(fc_ren_resultado_total_real_ml) ELSE 0 END AS fc_ren_orex ,
	NULL AS fc_ren_costemensualcto
FROM result_activo2
WHERE partition_date = '2020-06-30'
AND cod_contenido IN ('PLZ','CTA')
GROUP BY 
		--'ARG' AS cod_ren_unidad	 ,
		fec_data ,--S dt_ren_fec_data	 ,
		--NULL AS cod_ren_finalidad_datos	 ,
		cod_ren_contrato_rorac ,
		idf_cto_ods ,--AS cod_ren_contrato	 ,
		cod_area_negocio ,
		--'ARS' AS  cod_ren_divisa ,
		cod_ren_area_negocio_niv_9 ,--AS cod_ren_division ,
		cod_producto , --blce_result
		idf_pers_ods ,
		fec_alta_cto	 ,
		fec_ven	 ,
		cod_entidad_espana	 ,
		cod_centro_cont ,
		cod_subprodu ,
	    --NULL AS zona ,
	    --NULL AS territorial,
		cod_gestor_prod	 ,
		cod_gestor	 ,
		cod_vincula ,
		--NULL AS  mtm ,
	    --NULL AS  nocional ,
	    cod_divisa ,
	    --NULL AS flag_mora_local , --Flagmoralocal,
	    --NULL AS flag_carterizadas ,--FlagCarterizadas,
		nombre_cliente ,
		cod_tip_persona ,
	    --NULL AS nifcif , --NIFCIF,
	    cod_origen_inf ,--AS intragrupo ,
		--NULL AS cod_ren_internegocio , -- (resolver desde blce result) -- 
	    cod_ren_area_negocio_niv_9 ,--AS area_negocio_corp , 
		cod_producto_gest ,
		cod_segmento_gest ,
	    --cod_producto AS cod_producto_operacional,
		--NULL AS ds_ren_ficheromis ,
		fec_reestruc ,
		partition_date ,--AS dt_ren_fec_extr_datos ,
	    --NULL AS flagneteo ,
		--NULL AS fc_ren_costemensualcto,--AS fc_ren_costemensualcto ,
		cod_ren_producto_niv_3 ,
		cod_ren_producto_niv_4 ,
		cod_ren_cta_resultados_niv_8 , 
		cod_ren_cta_resultados_niv_9

-- /* CREATE TABLE COMMON */
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_ren_rorac_input_pasivo (	
	cod_ren_unidad string,
	dt_ren_data string,
	cod_ren_finalidaddatos string,
	cod_ren_contrato_rorac string,
	cod_ren_contrato string,
	cod_ren_areanegocio string,
	cod_ren_divisa string,
	cod_ren_division string,
	cod_ren_producto string,
	cod_ren_pers_ods string,
	fc_ren_margenmes DECIMAL(20,4),
	fc_ren_sdbsmv DECIMAL(20,4),
	fc_ren_sfbsmv DECIMAL(20,4),
	dt_ren_fec_altacto string,
	dt_ren_fec_ven string,
	cod_ren_entidad_espana string,
	cod_ren_centrocont string,
	cod_ren_subprodu string,
	cod_ren_zona string,
	cod_ren_territorial string,
	cod_ren_gestorprod string,
	cod_ren_gestor string,
	fc_ren_sdbsfm DECIMAL(20,4),
	fc_ren_interes DECIMAL(20,4),
	fc_ren_comfin DECIMAL(20,4),
	fc_ren_comnofin DECIMAL(20,4),
	cod_ren_vincula string,
	fc_ren_rof DECIMAL(20,4),
	fc_ren_tti DECIMAL(20,4),
	cod_ren_mtm string,
	cod_ren_nocional string,
	cod_ren_divisa_mis string,
	flag_ren_moralocal int,
	flag_ren_carterizadas int,
	ds_ren_nombrecliente string,
	cod_ren_tipopersona string,
	cod_ren_nifcif string,
	ds_ren_intragrupo string,
	cod_ren_internegocio string,
	cod_ren_areanegociocorp string,
	cod_productogest string,
	cod_segmentogest string,
	cod_producto_operacional string,
	ds_ren_ficheromis string,
	dt_ren_fec_reestruc string,
	dt_ren_fec_extrdatos string,
	flag_ren_neteo int,
	fc_ren_orex DECIMAL(20,4),
	fc_ren_costemensualcto DECIMAL(20,4)
)
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '/santander/bi-corp/common/rentabilidad/fact/bt_ren_rorac_input_pasivo'


-- /* INSERT PASIVO */
INSERT OVERWRITE TABLE bi_corp_common.bt_ren_rorac_input_pasivo 
PARTITION(partition_date='2020-06-30') 
SELECT cod_ren_unidad ,
dt_ren_fec_data ,
cod_ren_finalidad_datos ,
cod_ren_contrato_rorac ,
cod_ren_contrato ,
cod_area_negocio ,
cod_ren_divisa ,
cod_ren_division ,
cod_producto ,
idf_pers_ods ,
fc_ren_margenmes ,
fc_ren_sdbsmv ,
fc_ren_sfbsmv ,
fec_alta_cto ,
fec_ven ,
cod_entidad_espana ,
cod_centro_cont ,
cod_subprodu ,
zona ,
territorial ,
cod_gestor_prod ,
cod_gestor ,
fc_ren_sdbsfm ,
fc_ren_interes ,
fc_ren_comfin ,
fc_ren_comnofin ,
cod_vincula ,
fc_ren_rof ,
fc_ren_tti ,
mtm ,
nocional ,
cod_divisa ,
flag_mora_local ,
flag_carterizadas ,
nombre_cliente ,
cod_tip_persona ,
nifcif ,
intragrupo ,
cod_ren_internegocio ,
area_negocio_corp ,
cod_producto_gest ,
cod_segmento_gest ,
cod_producto_operacional ,
ds_ren_ficheromis ,
fec_reestruc ,
dt_ren_fec_extr_datos ,
flagneteo ,
fc_ren_orex ,
fc_ren_costemensualcto 
FROM result_pasivo_i ;


