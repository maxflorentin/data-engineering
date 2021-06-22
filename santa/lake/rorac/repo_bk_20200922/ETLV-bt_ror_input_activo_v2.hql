set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
--      _                       __                    __   _                        ___   
--     (_)____   ____   __  __ / /_     ____ _ _____ / /_ (_)_   __ ____     _   __|__ \  
--    / // __ \ / __ \ / / / // __/    / __ `// ___// __// /| | / // __ \   | | / /__/ /  
--   / // / / // /_/ // /_/ // /_     / /_/ // /__ / /_ / / | |/ // /_/ /   | |/ // __/ _ 
--  /_//_/ /_// .___/ \__,_/ \__/     \__,_/ \___/ \__//_/  |___/ \____/    |___//____/(_)
--           /_/      
----------------------------------------------------
-- IFRS9 TEMP
----------------------------------------------------
CREATE TEMPORARY TABLE IF NOT EXISTS tablon_ifrs9 AS  
SELECT CONCAT(RI.t_cod_entidad, RI.t_cod_centro, RI.t_cod_producto, lpad(RI.t_cod_subprodu_altair,4,'0'), RI.t_num_cuenta) cod_ren_contrato_short
	, RI.t_idf_cto_ifrs
	, RI.t_cod_entidad
	, RI.t_cod_centro
	, RI.t_num_cuenta
	, RI.t_cod_producto
	, lpad(RI.t_cod_subprodu_altair,4,'0') t_cod_subprodu_altair
	, RI.t_cod_divisa
	, SUM(RI.s_provision_amount) s_provision_amount
FROM santander_business_risk_ifrs9.ifrs9_tablon RI
WHERE RI.periodo = '20200630'
	AND RI.t_cod_divisa = 'ARS'
	AND RI.t_cod_entidad IS NOT NULL
	AND RI.t_cod_centro IS NOT NULL
	AND RI.t_cod_producto IS NOT NULL
	AND RI.t_cod_subprodu_altair IS NOT NULL
	AND RI.t_cod_divisa IS NOT NULL
GROUP BY RI.t_idf_cto_ifrs
	, RI.t_cod_entidad
	, RI.t_cod_centro
	, RI.t_num_cuenta
	, RI.t_cod_producto
	, lpad(RI.t_cod_subprodu_altair,4,'0') 
	, RI.t_cod_divisa ;

----------------------------------------------------
-- ROSETTA KEY 
----------------------------------------------------
CREATE TEMPORARY TABLE IF NOT EXISTS rosetta_key AS 
SELECT MK.native_key AS cod_ren_contrato_rorac
	 , BDR.native_key AS id_cto_bdr
FROM bi_corp_common.rosetta_nkey_hist MK
	JOIN bi_corp_common.rosetta_nkey_hist BDR ON 
		MK.master_key = BDR.master_key 
		AND BDR.partition_date = '2020-06-30'
		AND BDR.domain_code = '00004'
WHERE MK.partition_date = '2020-06-30'
	AND MK.domain_code = '00005';

----------------------------------------------------
-- RESULT TEMP | FLAG CONTRATO 
----------------------------------------------------
CREATE TEMPORARY TABLE IF NOT EXISTS input_activo AS 
SELECT 
	'ARG' AS cod_ren_unidad ,
	RE.dt_ren_fechadata , 
	7 AS cod_ren_finalidad_datos ,
	IF(RE.cod_ren_contenido = 'CCL', RE.cod_ren_cliente_rorac, RE.cod_ren_contrato_rorac) cod_ren_contrato_rorac ,
	RE.cod_ren_contrato ,
	RE.cod_ren_area_negocio ,
	'ARS' AS cod_divisa ,
	RE.cod_ren_area_negocio_niv_9 cod_ren_division ,
	RE.cod_ren_producto_generic AS cod_ren_producto , 
	RE.cod_ren_pers_ods ,
	CASE WHEN RE.cod_ren_producto_niv_3 = 'BG1'THEN SUM(RE.fc_ren_resultado_total_ml) ELSE 0 END AS fc_ren_margenmes ,
	CASE WHEN RE.cod_ren_producto_niv_3 = 'BG1'THEN SUM(RE.fc_ren_saldo_medio_total_ml) ELSE 0 END AS fc_ren_sdbsmv ,
	CASE WHEN RE.cod_ren_producto_niv_3 = 'BG3' THEN (SUM(RE.fc_ren_sdo_cap_med_ml)
										+ SUM(RE.fc_ren_sdo_med_int_ml)
										+ SUM(RE.fc_ren_sdo_med_reajuste_ml)
										+ SUM(RE.fc_ren_sdo_med_insolv_ml)) ELSE 0 END AS fc_ren_sfbsmv , 
	RE.dt_ren_altacontrato ,
	RE.dt_ren_vencontrato ,
	RE.cod_ren_entidad_espana ,
	RE.cod_ren_centro_cont ,
	RE.cod_ren_subproducto_generic ,
	NULL AS zona ,
	NULL AS territorial ,
	RE.cod_ren_gestor_prod ,
	RE.cod_ren_gestor ,
	CASE WHEN RE.cod_ren_producto_niv_3 = 'BG1' THEN (SUM(RE.fc_ren_ing_per_ml)
										+ SUM(RE.fc_ren_imp_ing_cap_ml)
										+ SUM(RE.fc_ren_sdo_cap_med_ml)
										+ SUM(RE.fc_ren_sdo_med_int_ml)
										+ SUM(RE.fc_ren_sdo_med_reajuste_ml)
										+ SUM(RE.fc_ren_sdo_med_insolv_ml)) ELSE 0 END AS fc_ren_sdbsfm ,
	CASE WHEN RE.cod_ren_producto_niv_3 = 'BG1' THEN SUM(RE.fc_ren_resultado_total_real_ml) ELSE 0 END AS fc_ren_interes ,
	CASE WHEN RE.cod_ren_cta_resultados_niv_9 = '1032' THEN SUM(RE.fc_ren_resultado_total_real_ml) ELSE 0 END AS fc_ren_comfin ,
	CASE WHEN RE.cod_ren_cta_resultados_niv_8 = '1051' THEN SUM(RE.fc_ren_resultado_total_real_ml) ELSE 0 END AS fc_ren_comnofin ,
	RE.cod_ren_vincula ,
	CASE WHEN RE.cod_ren_producto_niv_4 = 'LN004' THEN SUM(RE.fc_ren_resultado_total_real_ml) ELSE 0 END AS fc_ren_rof ,
	COALESCE(RE.fc_ren_out_tti,0) AS fc_ren_tti , 
	NULL AS  mtm ,
	NULL AS  nocional ,
	RE.cod_ren_divisa AS cod_ren_divisa_mis,
	NULL AS flag_mora_local , 
	NULL AS flag_carterizadas ,
	RE.ds_per_nombre_apellido ,
	RE.cod_per_tipopersona ,
	NULL AS nifcif , 
	RE.cod_ren_origen_inf ,
	NULL AS cod_ren_internegocio , -- (resolver desde blce result) -- 
	RE.cod_ren_area_negocio_niv_9 cod_area_negocio_corp , 
	RE.cod_ren_producto_gest ,
	RE.cod_ren_segmento_gest ,
	RE.cod_ren_producto_generic ,
	'20200630v1' AS ds_ren_ficheromis ,
	RE.dt_ren_reestruccontrato ,
	RE.partition_date AS dt_ren_fec_extr_datos ,
	NULL AS flagneteo ,
	CASE WHEN RE.cod_ren_cta_resultados_niv_8 = '1054' THEN SUM(RE.fc_ren_resultado_total_real_ml) ELSE 0 END AS fc_ren_orex ,
	NULL AS fc_ren_costemensualcto ,
	IF(RE.cod_ren_contenido = 'CCL', 1, 0) cod_nivel_operacion ,
	RE.partition_date ,
	ROW_NUMBER() OVER(PARTITION BY SUBSTRING(cod_ren_contrato,1,26)) order_contrato
FROM bi_corp_common.bt_ren_result RE
WHERE RE.partition_date = '2020-06-30'
	AND RE.flag_ren_perimetrororac = 1
	AND RE.cod_ren_producto_niv_3 IN ('BG1','BG3')
	AND RE.cod_ren_contenido IN ('PRE','CRE','ARF','CCO','COM','CCL','CTB','ROF') 
	AND RE.flag_ren_ind_pool = 0 
	AND RE.cod_ren_cta_resultados_niv_6 != '1073'
GROUP BY RE.dt_ren_fechadata , 
	IF(RE.cod_ren_contenido = 'CCL', RE.cod_ren_cliente_rorac, RE.cod_ren_contrato_rorac) ,
	RE.cod_ren_contrato ,
	RE.cod_ren_area_negocio ,
	RE.cod_ren_area_negocio_niv_9 ,
	RE.cod_ren_producto_generic , 
	RE.cod_ren_pers_ods ,
	RE.dt_ren_altacontrato ,
	RE.dt_ren_vencontrato ,
	RE.cod_ren_entidad_espana ,
	RE.cod_ren_centro_cont ,
	RE.cod_ren_subproducto_generic ,
	RE.cod_ren_gestor_prod ,
	RE.cod_ren_gestor ,
	RE.cod_ren_vincula ,
	RE.cod_ren_divisa ,
	RE.ds_per_nombre_apellido ,
	RE.cod_per_tipopersona ,
	RE.cod_ren_origen_inf ,
	RE.cod_ren_area_negocio_niv_9 ,
	RE.cod_ren_producto_gest ,
	RE.cod_ren_segmento_gest ,
	RE.cod_ren_producto_generic ,
	RE.dt_ren_reestruccontrato ,
	RE.partition_date ,
	--------------------------------
	RE.cod_ren_producto_niv_3 ,
	RE.cod_ren_producto_niv_4 ,
	RE.cod_ren_cta_resultados_niv_6 ,
	RE.cod_ren_cta_resultados_niv_8 ,
	RE.cod_ren_cta_resultados_niv_9 ,
	COALESCE(RE.fc_ren_out_tti,0),
	RE.cod_ren_contenido ;

----------------------------------------------------
-- INSERT TABLE
----------------------------------------------------
set mapred.job.queue.name=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE bi_corp_common.bt_ror_input_activo_v2
PARTITION(partition_date) 
SELECT RE.cod_ren_unidad ,
	RE.dt_ren_fechadata , 
	RE.cod_ren_finalidad_datos ,
	RE.cod_ren_contrato_rorac ,
	RE.cod_ren_contrato ,
	RE.cod_ren_area_negocio ,
	RE.cod_divisa ,
	RE.cod_ren_division ,
	RE.cod_ren_producto , 
	RE.cod_ren_pers_ods ,
	RE.fc_ren_margenmes ,
	RE.fc_ren_sdbsmv ,
	RE.fc_ren_sfbsmv , 
	RE.dt_ren_altacontrato ,
	RE.dt_ren_vencontrato ,
	RE.cod_ren_entidad_espana ,
	RE.cod_ren_centro_cont ,
	RE.cod_ren_subproducto_generic ,
	RE.zona ,
	RE.territorial ,
	RE.cod_ren_gestor_prod ,
	RE.cod_ren_gestor ,
	RK.id_cto_bdr ,
	RE.fc_ren_sdbsfm ,
	RE.fc_ren_interes ,
	RE.fc_ren_comfin ,
	RE.fc_ren_comnofin ,
	RE.cod_ren_vincula ,
	RE.fc_ren_rof ,
	RE.fc_ren_tti , 
	RE.mtm ,
	RE.nocional ,
	RE.cod_ren_divisa_mis ,
	RE.flag_mora_local , 
	RE.flag_carterizadas ,
	RE.ds_per_nombre_apellido ,
	RE.cod_per_tipopersona ,
	RE.nifcif , 
	RE.cod_ren_origen_inf ,
	RE.cod_ren_internegocio , 
	RE.cod_area_negocio_corp , 
	RE.cod_ren_producto_gest ,
	RE.cod_ren_segmento_gest ,
	RE.cod_ren_producto_generic ,
	RE.ds_ren_ficheromis ,
	RE.dt_ren_reestruccontrato ,
	RE.dt_ren_fec_extr_datos ,
	RE.flagneteo ,
	RE.fc_ren_orex ,
	RI.s_provision_amount AS fc_ifrs_provision ,
	RE.fc_ren_costemensualcto ,
	RE.cod_nivel_operacion ,	
	RE.partition_date
FROM input_activo RE
LEFT JOIN tablon_ifrs9 RI ON 
	SUBSTRING(RE.cod_ren_contrato,1,26) = RI.cod_ren_contrato_short
	AND RE.order_contrato = 1 
LEFT JOIN rosetta_key RK ON 
	RE.cod_ren_contrato_rorac = RK.cod_ren_contrato_rorac ;