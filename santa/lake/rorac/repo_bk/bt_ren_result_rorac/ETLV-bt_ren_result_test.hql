set mapred.job.queue.name=root.dataeng;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=40000000;
--      ____                __          __     _  __ _      __            __
--     / __ \ ___   ____   / /_ ____ _ / /_   (_)/ /(_)____/ /____ _ ____/ /
--    / /_/ // _ \ / __ \ / __// __ `// __ \ / // // // __  // __ `// __  / 
--   / _, _//  __// / / // /_ / /_/ // /_/ // // // // /_/ // /_/ // /_/ /  
--  /_/ |_| \___//_/ /_/ \__/ \__,_//_.___//_//_//_/ \__,_/ \__,_/ \__,_/   
--
--############################################################################################

-- GENERIC RESULT 
CREATE TEMPORARY TABLE generic_result AS
SELECT DISTINCT idf_cto_ods
	, cod_contenido
	, cod_pais
	, from_unixtime(unix_timestamp(fec_alta_cto , 'yyyyMMdd'), 'yyyy-MM-dd') fec_alta_cto
	, from_unixtime(unix_timestamp(fec_ven, 'yyyyMMdd'), 'yyyy-MM-dd') fec_ven
	, from_unixtime(unix_timestamp(fec_reestruc, 'yyyyMMdd'), 'yyyy-MM-dd') fec_reestruc
	, IF(cod_producto = 'null', null, cod_producto) cod_producto
	, IF(cod_subprodu = 'null', null, cod_subprodu) cod_subprodu
FROM bi_corp_staging.rio157_ms0_dt_dwh_generic_result 
WHERE partition_date = '2020-06-30' ;

----------------------------------------------------------------------------------------------
--############################################################################################

-- CLIENTE RESULT
CREATE TEMPORARY TABLE cliente_result AS
SELECT DISTINCT idf_pers_ods
	, CAST(SUBSTR(idf_pers_ods, 6, 8) AS INT) per_nup
	, cod_vincula
	, cod_tip_persona
	, CONCAT(IF(nom_nombre = 'null', '', nom_nombre),' ',IF(nom_apellido_1 = 'null', '', nom_apellido_1)) nombre_cliente
FROM bi_corp_staging.rio157_ms0_dt_dwh_cliente_result
WHERE partition_date = '2020-06-30' ;

----------------------------------------------------------------------------------------------
--############################################################################################

-- RESULT + PROD + ADN + CTA RESULT 
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=40000000;
set mapred.job.queue.name=root.dataeng;

CREATE TEMPORARY TABLE result_je AS
SELECT CONCAT(RE.cod_contenido,RE.idf_cto_ods) AS cod_ren_contrato_rorac ,
	CONCAT(RE.cod_contenido, RE.idf_pers_ods) cod_ren_cliente_rorac ,
	RE.* ,
	adn.cod_ren_area_negocio_niv_1,
	adn.cod_ren_area_negocio_niv_2,
	adn.cod_ren_area_negocio_niv_3,
	adn.cod_ren_area_negocio_niv_4,
	adn.cod_ren_area_negocio_niv_5,
	adn.cod_ren_area_negocio_niv_6,
	adn.cod_ren_area_negocio_niv_7,
	adn.cod_ren_area_negocio_niv_8,
	adn.cod_ren_area_negocio_niv_9,
	adn.ds_ren_area_negocio_niv_1,
	adn.ds_ren_area_negocio_niv_2,
	adn.ds_ren_area_negocio_niv_3,
	adn.ds_ren_area_negocio_niv_4,
	adn.ds_ren_area_negocio_niv_5,
	adn.ds_ren_area_negocio_niv_6,
	adn.ds_ren_area_negocio_niv_7,
	adn.ds_ren_area_negocio_niv_8,
	adn.ds_ren_area_negocio_niv_9,
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
	prod.ds_ren_producto_niv_10,
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
	cta.ds_ren_cta_resultados_niv_12,
	entj.des_hijo
FROM bi_corp_staging.rio157_ms0_ft_dwh_blce_result RE 
JOIN bi_corp_common.dim_ren_productosctrldn prod ON
	RE.cod_producto_gest = prod.cod_ren_producto 
JOIN bi_corp_common.dim_ren_areanegocioctr adn ON 
	RE.cod_area_negocio = adn.cod_ren_area_negocio 
JOIN bi_corp_staging.rio157_ms0_dm_je_dwh_entidades_ctr entj ON 
	entj.partition_date = '2020-07-14'
	AND RE.cod_pais = entj.cod_pais 
	AND entj.cod_hijo = '10001'
JOIN bi_corp_common.dim_ren_ctaresultadosctr cta ON
	RE.cod_cta_cont_gestion  = cta.cod_ren_cta_resultados
WHERE RE.partition_date = '2020-06-30';

-- RESULT + ENT -- DROP TABLE result_je2
CREATE TEMPORARY TABLE result_je2 AS
SELECT 
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
	ent.ds_ren_entidad_gest_niv_6,
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
	offi.ds_ren_ofi_comercial_niv_11 ,
	ges.cod_ren_gestor_niv_1 ,
	ges.cod_ren_gestor_niv_2 ,
	ges.cod_ren_gestor_niv_3 ,
	ges.cod_ren_gestor_niv_4 ,
	ges.cod_ren_gestor_niv_5 ,
	ges.cod_ren_gestor_niv_6 ,
	ges.cod_ren_gestor_niv_7 ,
	ges.cod_ren_gestor_niv_8 ,
	ges.cod_ren_gestor_niv_9 ,
	ges.cod_ren_gestor_niv_10 ,
	ges.cod_ren_gestor_niv_11 ,
	ges.ds_ren_gestor_niv_1 ,
	ges.ds_ren_gestor_niv_2 ,
	ges.ds_ren_gestor_niv_3 ,
	ges.ds_ren_gestor_niv_4 ,
	ges.ds_ren_gestor_niv_5 ,
	ges.ds_ren_gestor_niv_6 ,
	ges.ds_ren_gestor_niv_7 ,
	ges.ds_ren_gestor_niv_8 ,
	ges.ds_ren_gestor_niv_9 ,
	ges.ds_ren_gestor_niv_10 ,
	ges.ds_ren_gestor_niv_11
FROM result_je RE
JOIN bi_corp_common.dim_ren_entidadjuridica ent ON
	RE.cod_entidad_espana = ent.cod_ren_entidad_gest 
JOIN bi_corp_common.dim_ren_oficinas offi ON
	RE.cod_ofi_comercial = offi.cod_ren_ofi_comercial
LEFT JOIN bi_corp_common.dim_ren_gestor ges ON 
	RE.cod_gestor_prod = ges.cod_ren_gestor	;

----------------------------------------------------------------------------------------------
--############################################################################################
CREATE TEMPORARY TABLE result_g AS
SELECT RE.*
	, gen.cod_producto
	, gen.cod_subprodu
	, gen.fec_alta_cto
	, gen.fec_ven
	, gen.fec_reestruc
FROM result_je2 RE
JOIN generic_result gen ON 
	gen.cod_contenido NOT IN ('GAS','RSM','CTB','CCL')
	AND RE.idf_cto_ods = gen.idf_cto_ods
	AND RE.cod_contenido = gen.cod_contenido 
WHERE gen.cod_contenido NOT IN ('GAS','RSM','CTB','CCL')
AND RE.cod_contenido NOT IN ('GAS','RSM','CTB','CCL')
UNION ALL
SELECT RE.*
	, NULL cod_producto
	, NULL cod_subprodu
	, NULL fec_alta_cto
	, NULL fec_ven
	, NULL fec_reestruc
FROM result_je2 RE
WHERE RE.cod_contenido IN ('GAS','RSM','CTB','CCL');

----------------------------------------------------------------------------------------------
--############################################################################################
CREATE TEMPORARY TABLE result_gc AS
SELECT RE.*	
	, cli.cod_vincula
	, cli.cod_tip_persona
	, cli.nombre_cliente
FROM result_g RE
JOIN cliente_result cli ON
	cli.idf_pers_ods = RE.idf_pers_ods ;

----------------------------------------------------------------------------------------------
--############################################################################################
-- DROP TEMP
DROP TABLE IF EXISTS generic_result ;
DROP TABLE IF EXISTS cliente_result ;
DROP TABLE IF EXISTS result_g ;

----------------------------------------------------------------------------------------------
--      _                           __     __          __     __    
--     (_)____   _____ ___   _____ / /_   / /_ ____ _ / /_   / /___ 
--    / // __ \ / ___// _ \ / ___// __/  / __// __ `// __ \ / // _ \
--   / // / / /(__  )/  __// /   / /_   / /_ / /_/ // /_/ // //  __/
--  /_//_/ /_//____/ \___//_/    \__/   \__/ \__,_//_.___//_/ \___/ 
--                                                                  
--############################################################################################
INSERT OVERWRITE TABLE bi_corp_common.bt_ren_result_test
PARTITION(partition_date) 
SELECT RE.idf_cto_ods cod_ren_contrato,
	RE.fec_alta_cto dt_ren_altacontrato,
	RE.fec_ven dt_ren_vencontrato,
	RE.fec_reestruc dt_ren_reestruccontrato,
	RE.cod_producto cod_ren_producto_generic,
	RE.cod_subprodu cod_ren_subproducto_generic,
	RE.cod_contenido cod_ren_contenido,
	RE.fec_data dt_ren_fechadata,
	RE.cod_pais cod_ren_pais,
	RE.idf_pers_ods,
	CAST(SUBSTRING(RE.idf_pers_ods,6,8)AS INT) cod_per_nup,
	RE.cod_vincula cod_ren_vincula,
	RE.cod_tip_persona cod_per_tipopersona,
	RE.nombre_cliente ds_per_nombre_apellido,
	RE.cod_entidad_espana cod_ren_entidad_espana,
	RE.cod_producto_gest cod_ren_producto_gest,
	RE.cod_divisa cod_ren_divisa,
	IF(RE.cod_reajuste = 'null', null, RE.cod_reajuste) cod_ren_reajuste,
	RE.cod_agrp_func cod_ren_agrp_func,
	RE.cod_est_sdo cod_ren_est_sdo,
	RE.cod_cta_cont_gestion cod_ren_cont_gestion,
	RE.cod_area_negocio cod_ren_area_negocio,
	RE.cod_tip_ajuste cod_ren_tipo_ajuste,
	RE.cod_centro_cont cod_ren_centro_cont,
	RE.cod_ofi_comercial cod_ren_ofi_comercial,
	RE.ind_conciliacion cod_ren_conciliacion,
	IF(RE.ind_pool = 'S',1,0) flag_ren_ind_pool,
	RE.cod_segmento_gest cod_ren_segmento_gest,
	RE.cod_gestor cod_ren_gestor,
	RE.cod_univ cod_ren_univ,
	IF(RE.cod_gestor_prod = 'null', null, RE.cod_gestor_prod) cod_ren_gestor_prod,
	RE.cod_origen_inf cod_ren_origen_inf,
	RE.cod_ren_producto_niv_1 ,
	RE.cod_ren_producto_niv_2 ,
	RE.cod_ren_producto_niv_3 ,
	RE.cod_ren_producto_niv_4 ,
	RE.cod_ren_producto_niv_5 ,
	RE.cod_ren_producto_niv_6 ,
	RE.cod_ren_producto_niv_7 ,
	RE.cod_ren_producto_niv_8 ,
	RE.cod_ren_producto_niv_9 ,
	RE.cod_ren_producto_niv_10 ,
	RE.ds_ren_producto_niv_1 ,
	RE.ds_ren_producto_niv_2 ,
	RE.ds_ren_producto_niv_3 ,
	RE.ds_ren_producto_niv_4 ,
	RE.ds_ren_producto_niv_5 ,
	RE.ds_ren_producto_niv_6 ,
	RE.ds_ren_producto_niv_7 ,
	RE.ds_ren_producto_niv_8 ,
	RE.ds_ren_producto_niv_9 ,
	RE.ds_ren_producto_niv_10 ,
	RE.cod_ren_cta_resultados_niv_1 ,
	RE.cod_ren_cta_resultados_niv_2 ,
	RE.cod_ren_cta_resultados_niv_3 ,
	RE.cod_ren_cta_resultados_niv_4 ,
	RE.cod_ren_cta_resultados_niv_5 ,
	RE.cod_ren_cta_resultados_niv_6 ,
	RE.cod_ren_cta_resultados_niv_7 ,
	RE.cod_ren_cta_resultados_niv_8 ,
	RE.cod_ren_cta_resultados_niv_9 ,
	RE.cod_ren_cta_resultados_niv_10 ,
	RE.cod_ren_cta_resultados_niv_11 ,
	RE.cod_ren_cta_resultados_niv_12 ,
	RE.ds_ren_cta_resultados_niv_1 ,
	RE.ds_ren_cta_resultados_niv_2 ,
	RE.ds_ren_cta_resultados_niv_3 ,
	RE.ds_ren_cta_resultados_niv_4 ,
	RE.ds_ren_cta_resultados_niv_5 ,
	RE.ds_ren_cta_resultados_niv_6 ,
	RE.ds_ren_cta_resultados_niv_7 ,
	RE.ds_ren_cta_resultados_niv_8 ,
	RE.ds_ren_cta_resultados_niv_9 ,
	RE.ds_ren_cta_resultados_niv_10 ,
	RE.ds_ren_cta_resultados_niv_11 ,
	RE.ds_ren_cta_resultados_niv_12 ,
	RE.cod_ren_area_negocio_niv_1 ,
	RE.cod_ren_area_negocio_niv_2 ,
	RE.cod_ren_area_negocio_niv_3 ,
	RE.cod_ren_area_negocio_niv_4 ,
	RE.cod_ren_area_negocio_niv_5 ,
	RE.cod_ren_area_negocio_niv_6 ,
	RE.cod_ren_area_negocio_niv_7 ,
	RE.cod_ren_area_negocio_niv_8 ,
	RE.cod_ren_area_negocio_niv_9 ,
	RE.ds_ren_area_negocio_niv_1 ,
	RE.ds_ren_area_negocio_niv_2 ,
	RE.ds_ren_area_negocio_niv_3 ,
	RE.ds_ren_area_negocio_niv_4 ,
	RE.ds_ren_area_negocio_niv_5 ,
	RE.ds_ren_area_negocio_niv_6 ,
	RE.ds_ren_area_negocio_niv_7 ,
	RE.ds_ren_area_negocio_niv_8 ,
	RE.ds_ren_area_negocio_niv_9 ,
	RE.cod_ren_entidad_gest_niv_1 ,
	RE.cod_ren_entidad_gest_niv_2 ,
	RE.cod_ren_entidad_gest_niv_3 ,
	RE.cod_ren_entidad_gest_niv_4 ,
	RE.cod_ren_entidad_gest_niv_5 ,
	RE.cod_ren_entidad_gest_niv_6 ,
	RE.ds_ren_entidad_gest_niv_1 ,
	RE.ds_ren_entidad_gest_niv_2 ,
	RE.ds_ren_entidad_gest_niv_3 ,
	RE.ds_ren_entidad_gest_niv_4 ,
	RE.ds_ren_entidad_gest_niv_5 ,
	RE.ds_ren_entidad_gest_niv_6 ,
	RE.cod_ren_ofi_comercial_niv_5 ,
	RE.cod_ren_ofi_comercial_niv_6 ,
	RE.cod_ren_ofi_comercial_niv_7 ,
	RE.cod_ren_ofi_comercial_niv_8 ,
	RE.cod_ren_ofi_comercial_niv_9 ,
	RE.cod_ren_ofi_comercial_niv_10 ,
	RE.cod_ren_ofi_comercial_niv_11 ,
	RE.ds_ren_ofi_comercial_niv_5 ,
	RE.ds_ren_ofi_comercial_niv_6 ,
	RE.ds_ren_ofi_comercial_niv_7 ,
	RE.ds_ren_ofi_comercial_niv_8 ,
	RE.ds_ren_ofi_comercial_niv_9 ,
	RE.ds_ren_ofi_comercial_niv_10 ,
	RE.ds_ren_ofi_comercial_niv_11 ,
	RE.cod_ren_gestor_niv_1 ,
	RE.cod_ren_gestor_niv_2 ,
	RE.cod_ren_gestor_niv_3 ,
	RE.cod_ren_gestor_niv_4 ,
	RE.cod_ren_gestor_niv_5 ,
	RE.cod_ren_gestor_niv_6 ,
	RE.cod_ren_gestor_niv_7 ,
	RE.cod_ren_gestor_niv_8 ,
	RE.cod_ren_gestor_niv_9 ,
	RE.cod_ren_gestor_niv_10 ,
	RE.cod_ren_gestor_niv_11 ,
	RE.ds_ren_gestor_niv_1 ,
	RE.ds_ren_gestor_niv_2 ,
	RE.ds_ren_gestor_niv_3 ,
	RE.ds_ren_gestor_niv_4 ,
	RE.ds_ren_gestor_niv_5 ,
	RE.ds_ren_gestor_niv_6 ,
	RE.ds_ren_gestor_niv_7 ,
	RE.ds_ren_gestor_niv_8 ,
	RE.ds_ren_gestor_niv_9 ,
	RE.ds_ren_gestor_niv_10 ,
	RE.ds_ren_gestor_niv_11 ,
	sum(COALESCE(RE.out_tti, 0)) fc_ren_out_tti,
	sum(COALESCE(RE.imp_ing_per_ml, 0)) fc_ren_imp_ing_per_ml,
	sum(COALESCE(RE.imp_ing_cap_ml, 0)) fc_ren_imp_ing_cap_ml,
	sum(COALESCE(RE.imp_ing_reaj_ml, 0)) fc_ren_ing_reaj_ml,
	sum(COALESCE(RE.imp_egr_per_ml, 0)) fc_ren_egr_per_ml,
	sum(COALESCE(RE.imp_egr_cap_ml, 0)) fc_ren_egr_cap_ml,
	sum(COALESCE(RE.imp_egr_reaj_ml, 0)) fc_ren_egr_reaj_ml,
	sum(COALESCE(RE.imp_ajtti_egr_tb_cap_ml, 0)) fc_ren_ajtti_egr_tb_cap_ml,
	sum(COALESCE(RE.imp_ajtti_egr_sl_cap_ml, 0)) fc_ren_ajtti_egr_sl_cap_ml,
	sum(COALESCE(RE.imp_ajtti_egr_per_ml, 0)) fc_ren_ajtti_egr_per_ml,
	sum(COALESCE(RE.imp_ajtti_egr_reajuste_ml, 0))  fc_ren_ajtti_egr_reajuste_ml,
	sum(COALESCE(RE.imp_ajtti_ing_tb_cap_ml, 0))  fc_ren_ajtti_ing_tb_cap_ml,
	sum(COALESCE(RE.imp_ajtti_ing_sl_cap_ml, 0))  fc_ren_ajtti_ing_sl_cap_ml,
	sum(COALESCE(RE.imp_ajtti_ing_per_ml, 0)) fc_ren_ajtti_ing_per_ml,
	sum(COALESCE(RE.imp_ajtti_ing_reajuste_ml, 0))  fc_ren_ajtti_ing_reajuste_ml,
	sum(COALESCE(RE.imp_efec_enc_ml, 0))  fc_ren_efec_enc_ml,
	sum(COALESCE(RE.imp_ing_per_mo, 0)) fc_ren_ing_per_mo,
	sum(COALESCE(RE.imp_ing_cap_mo, 0)) fc_ren_ing_cap_mo,
	sum(COALESCE(RE.imp_ing_reaj_mo, 0))  fc_ren_ing_reaj_mo,
	sum(COALESCE(RE.imp_egr_per_mo, 0)) fc_ren_egr_per_mo,
	sum(COALESCE(RE.imp_egr_cap_mo, 0))  fc_ren_egr_cap_mo,
	sum(COALESCE(RE.imp_egr_reaj_mo, 0)) fc_ren_egr_reaj_mo,
	sum(COALESCE(RE.imp_ajtti_egr_tb_cap_mo, 0))  fc_ren_ajtti_egr_tb_cap_mo,
	sum(COALESCE(RE.imp_ajtti_egr_sl_cap_mo, 0)) fc_ren_ajtti_egr_sl_cap_mo,
	sum(COALESCE(RE.imp_ajtti_egr_per_mo, 0))  fc_ren_ajtti_egr_per_mo,
	sum(COALESCE(RE.imp_ajtti_egr_reajuste_mo, 0))  fc_ren_ajtti_egr_reajuste_mo,
	sum(COALESCE(RE.imp_ajtti_ing_tb_cap_mo, 0))  fc_ren_ajtti_ing_tb_cap_mo,
	sum(COALESCE(RE.imp_ajtti_ing_sl_cap_mo, 0))  fc_ren_ajtti_ing_sl_cap_mo,
	sum(COALESCE(RE.imp_ajtti_ing_per_mo, 0)) fc_ren_ajtti_ing_per_mo,
	sum(COALESCE(RE.imp_ajtti_ing_reajuste_mo, 0))  fc_ren_ajtti_ing_reajuste_mo,
	sum(COALESCE(RE.imp_efec_enc_mo, 0))  fc_ren_efec_enc_mo,
	sum(COALESCE(RE.imp_sdo_cap_med_ml, 0))  fc_ren_sdo_cap_med_ml,
	sum(COALESCE(RE.imp_sdo_med_int_ml, 0))  fc_ren_sdo_med_int_ml,
	sum(COALESCE(RE.imp_sdo_med_reajuste_ml, 0))  fc_ren_sdo_med_reajuste_ml,
	sum(COALESCE(RE.imp_sdo_med_insolv_ml, 0))  fc_ren_sdo_med_insolv_ml,
	sum(COALESCE(RE.imp_sdo_cap_med_mo, 0))  fc_ren_sdo_cap_med_mo,
	sum(COALESCE(RE.imp_sdo_med_int_mo, 0))  fc_ren_sdo_med_int_mo,
	sum(COALESCE(RE.imp_sdo_med_reajuste_mo, 0)) fc_ren_sdo_med_reajuste_mo,
	sum(COALESCE(RE.imp_sdo_med_insolv_mo, 0))  fc_ren_sdo_med_insolv_mo,
	sum(COALESCE(RE.imp_ing_per_ml, 0))  fc_ren_ing_per_ml,
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
	        + sum(COALESCE(RE.IMP_EFEC_ENC_ML, 0))) fc_ren_resultado_total_ml ,
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
	        + sum(COALESCE(RE.IMP_EFEC_ENC_MO, 0))) fc_ren_resultado_total_mo ,
	(sum(COALESCE(RE.IMP_SDO_CAP_MED_ML, 0)) 
	        + sum(COALESCE(RE.IMP_SDO_MED_INT_ML, 0)) 
	        + sum(COALESCE(RE.IMP_SDO_MED_REAJUSTE_ML, 0)) 
	        + sum(COALESCE(RE.IMP_SDO_MED_INSOLV_ML, 0))) fc_ren_saldo_medio_total_ml ,
	(sum(COALESCE(RE.IMP_SDO_CAP_MED_MO, 0)) 
	        + sum(COALESCE(RE.IMP_SDO_MED_INT_MO, 0)) 
	        + sum(COALESCE(RE.IMP_SDO_MED_REAJUSTE_MO, 0)) 
	        + sum(COALESCE(RE.IMP_SDO_MED_INSOLV_MO, 0))) fc_ren_saldo_medio_total_mo ,
	(sum(COALESCE(RE.IMP_ING_PER_ML, 0)) 
	        + sum(COALESCE(RE.IMP_ING_CAP_ML, 0)) 
	        + sum(COALESCE(RE.IMP_ING_REAJ_ML, 0)) 
	        + sum(COALESCE(RE.IMP_EGR_PER_ML, 0)) 
	        + sum(COALESCE(RE.IMP_EGR_CAP_ML, 0)) 
	        + sum(COALESCE(RE.IMP_EGR_REAJ_ML, 0))) fc_ren_resultado_total_real_ml ,
	(sum(COALESCE(RE.IMP_ING_PER_MO, 0)) 
	        + sum(COALESCE(RE.IMP_ING_CAP_MO, 0)) 
	        + sum(COALESCE(RE.IMP_ING_REAJ_MO, 0)) 
	        + sum(COALESCE(RE.IMP_EGR_PER_MO, 0)) 
	        + sum(COALESCE(RE.IMP_EGR_CAP_MO, 0)) 
	        + sum(COALESCE(RE.IMP_EGR_REAJ_MO, 0))) fc_ren_resultado_total_real_mo ,
	(sum(COALESCE(RE.IMP_SDO_CAP_ML, 0)) 
	        + sum(COALESCE(RE.IMP_SDO_INT_ML, 0)) 
	        + sum(COALESCE(RE.IMP_SDO_INSOLV_ML, 0)) 
	        + sum(COALESCE(RE.IMP_SDO_REAJUSTE_ML, 0))) fc_ren_saldo_puntual_ml ,
	(sum(COALESCE(RE.IMP_SDO_CAP_MO, 0)) 
	        + sum(COALESCE(RE.IMP_SDO_INT_MO, 0)) 
	        + sum(COALESCE(RE.IMP_SDO_INSOLV_MO, 0)) 
	        + sum(COALESCE(RE.IMP_SDO_REAJUSTE_MO, 0))) fc_ren_saldo_puntual_mo ,
	IF(RE.imp_sdo_cap_ml != '0' OR RE.imp_sdo_cap_mo != '0' OR RE.imp_sdo_int_ml != '0' OR RE.imp_sdo_int_mo != '0' OR RE.imp_sdo_reajuste_ml != '0' OR RE.imp_sdo_reajuste_mo != '0'
	OR RE.imp_sdo_insolv_ml        != '0' OR RE.imp_sdo_insolv_mo != '0' OR RE.imp_sdo_cap_med_ml != '0' OR RE.imp_sdo_cap_med_mo != '0' OR RE.imp_sdo_med_int_ml != '0' OR RE.imp_sdo_med_int_mo != '0'
	OR RE.imp_sdo_med_reajuste_ml != '0' OR RE.imp_sdo_med_reajuste_mo != '0' OR RE.imp_sdo_med_insolv_ml != '0'
	OR RE.imp_sdo_med_insolv_mo        != '0' OR RE.imp_egr_cap_ml        != '0' OR RE.imp_egr_cap_mo        != '0' OR RE.imp_egr_per_ml != '0' OR RE.imp_egr_per_mo != '0' OR RE.imp_egr_reaj_ml != '0'
	OR RE.imp_egr_reaj_mo != '0' OR RE.imp_ing_cap_ml != '0' OR RE.imp_ing_cap_mo != '0' OR RE.imp_ing_per_ml != '0' OR RE.imp_ing_per_mo != '0' OR RE.imp_ing_reaj_ml != '0'
	OR RE.imp_ing_reaj_mo != '0' OR RE.imp_ajtti_egr_tb_cap_ml != '0' OR RE.imp_ajtti_egr_tb_cap_mo != '0'
	OR RE.imp_ajtti_egr_sl_cap_ml != '0' OR RE.imp_ajtti_egr_sl_cap_mo != '0' OR RE.imp_ajtti_egr_per_ml != '0'
	OR RE.imp_ajtti_egr_per_mo != '0' OR RE.imp_ajtti_egr_reajuste_ml != '0' OR RE.imp_ajtti_egr_reajuste_mo != '0'
	OR RE.imp_ajtti_ing_tb_cap_ml != '0' OR RE.imp_ajtti_ing_tb_cap_mo != '0' OR RE.imp_ajtti_ing_sl_cap_ml != '0'
	OR RE.imp_ajtti_ing_sl_cap_mo != '0' OR RE.imp_ajtti_ing_per_ml        != '0' OR RE.imp_ajtti_ing_per_mo != '0'
	OR RE.imp_ajtti_ing_reajuste_ml        != '0' OR RE.imp_ajtti_ing_reajuste_mo != '0' OR RE.imp_efec_enc_ml != '0'
	OR RE.imp_efec_enc_mo != '0' ,1,0) flag_ren_perimetrororac ,
	RE.cod_ren_contrato_rorac ,
	RE.cod_ren_cliente_rorac ,
	RE.partition_date
FROM result_gc RE  
GROUP BY RE.idf_cto_ods ,
	RE.fec_alta_cto ,
	RE.fec_ven ,
	RE.fec_reestruc ,
	RE.cod_producto ,
	RE.cod_subprodu ,
	RE.cod_contenido ,
	RE.fec_data ,
	RE.cod_pais ,
	RE.idf_pers_ods,
	CAST(SUBSTRING(RE.idf_pers_ods,6,8)AS INT) ,
	RE.cod_vincula ,
	RE.cod_tip_persona ,
	RE.nombre_cliente ,
	RE.cod_entidad_espana ,
	RE.cod_producto_gest ,
	RE.cod_divisa ,
	IF(RE.cod_reajuste = 'null', null, RE.cod_reajuste) ,
	RE.cod_agrp_func ,
	RE.cod_est_sdo ,
	RE.cod_cta_cont_gestion ,
	RE.cod_area_negocio ,
	RE.cod_tip_ajuste ,
	RE.cod_centro_cont ,
	RE.cod_ofi_comercial ,
	RE.ind_conciliacion ,
	IF(RE.ind_pool = 'S',1,0) ,
	RE.cod_segmento_gest ,
	RE.cod_gestor ,
	RE.cod_univ ,
	IF(RE.cod_gestor_prod = 'null', null, RE.cod_gestor_prod) ,
	RE.cod_origen_inf ,
	RE.cod_ren_producto_niv_1 ,
	RE.cod_ren_producto_niv_2 ,
	RE.cod_ren_producto_niv_3 ,
	RE.cod_ren_producto_niv_4 ,
	RE.cod_ren_producto_niv_5 ,
	RE.cod_ren_producto_niv_6 ,
	RE.cod_ren_producto_niv_7 ,
	RE.cod_ren_producto_niv_8 ,
	RE.cod_ren_producto_niv_9 ,
	RE.cod_ren_producto_niv_10 ,
	RE.ds_ren_producto_niv_1 ,
	RE.ds_ren_producto_niv_2 ,
	RE.ds_ren_producto_niv_3 ,
	RE.ds_ren_producto_niv_4 ,
	RE.ds_ren_producto_niv_5 ,
	RE.ds_ren_producto_niv_6 ,
	RE.ds_ren_producto_niv_7 ,
	RE.ds_ren_producto_niv_8 ,
	RE.ds_ren_producto_niv_9 ,
	RE.ds_ren_producto_niv_10 ,
	RE.cod_ren_cta_resultados_niv_1 ,
	RE.cod_ren_cta_resultados_niv_2 ,
	RE.cod_ren_cta_resultados_niv_3 ,
	RE.cod_ren_cta_resultados_niv_4 ,
	RE.cod_ren_cta_resultados_niv_5 ,
	RE.cod_ren_cta_resultados_niv_6 ,
	RE.cod_ren_cta_resultados_niv_7 ,
	RE.cod_ren_cta_resultados_niv_8 ,
	RE.cod_ren_cta_resultados_niv_9 ,
	RE.cod_ren_cta_resultados_niv_10 ,
	RE.cod_ren_cta_resultados_niv_11 ,
	RE.cod_ren_cta_resultados_niv_12 ,
	RE.ds_ren_cta_resultados_niv_1 ,
	RE.ds_ren_cta_resultados_niv_2 ,
	RE.ds_ren_cta_resultados_niv_3 ,
	RE.ds_ren_cta_resultados_niv_4 ,
	RE.ds_ren_cta_resultados_niv_5 ,
	RE.ds_ren_cta_resultados_niv_6 ,
	RE.ds_ren_cta_resultados_niv_7 ,
	RE.ds_ren_cta_resultados_niv_8 ,
	RE.ds_ren_cta_resultados_niv_9 ,
	RE.ds_ren_cta_resultados_niv_10 ,
	RE.ds_ren_cta_resultados_niv_11 ,
	RE.ds_ren_cta_resultados_niv_12 ,
	RE.cod_ren_area_negocio_niv_1 ,
	RE.cod_ren_area_negocio_niv_2 ,
	RE.cod_ren_area_negocio_niv_3 ,
	RE.cod_ren_area_negocio_niv_4 ,
	RE.cod_ren_area_negocio_niv_5 ,
	RE.cod_ren_area_negocio_niv_6 ,
	RE.cod_ren_area_negocio_niv_7 ,
	RE.cod_ren_area_negocio_niv_8 ,
	RE.cod_ren_area_negocio_niv_9 ,
	RE.ds_ren_area_negocio_niv_1 ,
	RE.ds_ren_area_negocio_niv_2 ,
	RE.ds_ren_area_negocio_niv_3 ,
	RE.ds_ren_area_negocio_niv_4 ,
	RE.ds_ren_area_negocio_niv_5 ,
	RE.ds_ren_area_negocio_niv_6 ,
	RE.ds_ren_area_negocio_niv_7 ,
	RE.ds_ren_area_negocio_niv_8 ,
	RE.ds_ren_area_negocio_niv_9 ,
	RE.cod_ren_entidad_gest_niv_1 ,
	RE.cod_ren_entidad_gest_niv_2 ,
	RE.cod_ren_entidad_gest_niv_3 ,
	RE.cod_ren_entidad_gest_niv_4 ,
	RE.cod_ren_entidad_gest_niv_5 ,
	RE.cod_ren_entidad_gest_niv_6 ,
	RE.ds_ren_entidad_gest_niv_1 ,
	RE.ds_ren_entidad_gest_niv_2 ,
	RE.ds_ren_entidad_gest_niv_3 ,
	RE.ds_ren_entidad_gest_niv_4 ,
	RE.ds_ren_entidad_gest_niv_5 ,
	RE.ds_ren_entidad_gest_niv_6 ,
	RE.cod_ren_ofi_comercial_niv_5 ,
	RE.cod_ren_ofi_comercial_niv_6 ,
	RE.cod_ren_ofi_comercial_niv_7 ,
	RE.cod_ren_ofi_comercial_niv_8 ,
	RE.cod_ren_ofi_comercial_niv_9 ,
	RE.cod_ren_ofi_comercial_niv_10 ,
	RE.cod_ren_ofi_comercial_niv_11 ,
	RE.ds_ren_ofi_comercial_niv_5 ,
	RE.ds_ren_ofi_comercial_niv_6 ,
	RE.ds_ren_ofi_comercial_niv_7 ,
	RE.ds_ren_ofi_comercial_niv_8 ,
	RE.ds_ren_ofi_comercial_niv_9 ,
	RE.ds_ren_ofi_comercial_niv_10 ,
	RE.ds_ren_ofi_comercial_niv_11 ,
	RE.cod_ren_gestor_niv_1 ,
	RE.cod_ren_gestor_niv_2 ,
	RE.cod_ren_gestor_niv_3 ,
	RE.cod_ren_gestor_niv_4 ,
	RE.cod_ren_gestor_niv_5 ,
	RE.cod_ren_gestor_niv_6 ,
	RE.cod_ren_gestor_niv_7 ,
	RE.cod_ren_gestor_niv_8 ,
	RE.cod_ren_gestor_niv_9 ,
	RE.cod_ren_gestor_niv_10 ,
	RE.cod_ren_gestor_niv_11 ,
	RE.ds_ren_gestor_niv_1 ,
	RE.ds_ren_gestor_niv_2 ,
	RE.ds_ren_gestor_niv_3 ,
	RE.ds_ren_gestor_niv_4 ,
	RE.ds_ren_gestor_niv_5 ,
	RE.ds_ren_gestor_niv_6 ,
	RE.ds_ren_gestor_niv_7 ,
	RE.ds_ren_gestor_niv_8 ,
	RE.ds_ren_gestor_niv_9 ,
	RE.ds_ren_gestor_niv_10 ,
	RE.ds_ren_gestor_niv_11 ,
	IF(RE.imp_sdo_cap_ml != '0' OR RE.imp_sdo_cap_mo != '0' OR RE.imp_sdo_int_ml != '0' OR RE.imp_sdo_int_mo != '0' OR RE.imp_sdo_reajuste_ml != '0' OR RE.imp_sdo_reajuste_mo != '0'
	OR RE.imp_sdo_insolv_ml != '0' OR RE.imp_sdo_insolv_mo != '0' OR RE.imp_sdo_cap_med_ml != '0' OR RE.imp_sdo_cap_med_mo != '0' OR RE.imp_sdo_med_int_ml != '0' OR RE.imp_sdo_med_int_mo != '0'
	OR RE.imp_sdo_med_reajuste_ml != '0' OR RE.imp_sdo_med_reajuste_mo != '0' OR RE.imp_sdo_med_insolv_ml != '0'
	OR RE.imp_sdo_med_insolv_mo != '0' OR RE.imp_egr_cap_ml != '0' OR RE.imp_egr_cap_mo != '0' OR RE.imp_egr_per_ml != '0' OR RE.imp_egr_per_mo != '0' OR RE.imp_egr_reaj_ml != '0'
	OR RE.imp_egr_reaj_mo != '0' OR RE.imp_ing_cap_ml != '0' OR RE.imp_ing_cap_mo != '0' OR RE.imp_ing_per_ml != '0' OR RE.imp_ing_per_mo != '0' OR RE.imp_ing_reaj_ml != '0'
	OR RE.imp_ing_reaj_mo != '0' OR RE.imp_ajtti_egr_tb_cap_ml != '0' OR RE.imp_ajtti_egr_tb_cap_mo != '0'
	OR RE.imp_ajtti_egr_sl_cap_ml != '0' OR RE.imp_ajtti_egr_sl_cap_mo != '0' OR RE.imp_ajtti_egr_per_ml != '0'
	OR RE.imp_ajtti_egr_per_mo != '0' OR RE.imp_ajtti_egr_reajuste_ml != '0' OR RE.imp_ajtti_egr_reajuste_mo != '0'
	OR RE.imp_ajtti_ing_tb_cap_ml != '0' OR RE.imp_ajtti_ing_tb_cap_mo != '0' OR RE.imp_ajtti_ing_sl_cap_ml != '0'
	OR RE.imp_ajtti_ing_sl_cap_mo != '0' OR RE.imp_ajtti_ing_per_ml != '0' OR RE.imp_ajtti_ing_per_mo != '0'
	OR RE.imp_ajtti_ing_reajuste_ml != '0' OR RE.imp_ajtti_ing_reajuste_mo != '0' OR RE.imp_efec_enc_ml != '0'
	OR RE.imp_efec_enc_mo != '0' ,1,0) ,
	RE.cod_ren_contrato_rorac ,
	RE.cod_ren_cliente_rorac ,
	RE.partition_date ;