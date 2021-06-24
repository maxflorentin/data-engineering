set mapred.job.queue.name=root.dataeng;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=40000000; 
--      ___          __   _                  ___   ___ 
--     /   |  _____ / /_ (_)_   __ ____     <  /  |__ \
--    / /| | / ___// __// /| | / // __ \    / /   __/ /
--   / ___ |/ /__ / /_ / / | |/ // /_/ /   / /_  / __/ 
--  /_/  |_|\___/ \__//_/  |___/ \____/   /_/(_)/____/ 
--                                                  
-------------------------------------------------------------------------
-- ACTIVO_RESULT --
CREATE TEMPORARY TABLE activo_result AS  
SELECT 
	CONCAT(RE.cod_contenido,RE.idf_cto_ods) AS cod_ren_contrato_rorac ,
	CONCAT(RE.cod_contenido,RE.idf_pers_ods) cod_ren_cliente_rorac ,
	RE.idf_cto_ods ,
	RE.cod_contenido ,
	from_unixtime(unix_timestamp(RE.fec_data,'yyyyMMdd'),'yyyy-MM-dd') fec_data ,
	RE.cod_pais ,
	RE.idf_pers_ods ,
	CAST(SUBSTRING(RE.idf_pers_ods,6,8) AS INT) AS per_nup ,
	RE.cod_entidad_espana ,
	RE.cod_producto_gest ,
	RE.cod_divisa ,
	IF(RE.cod_reajuste = 'null', cast(NULL as string), RE.cod_reajuste) cod_reajuste ,
	RE.cod_agrp_func ,
	RE.cod_est_sdo ,
	RE.cod_cta_cont_gestion ,
	RE.cod_area_negocio ,
	RE.cod_tip_ajuste ,
	RE.cod_centro_cont ,
	RE.cod_ofi_comercial ,
	RE.ind_conciliacion ,
	RE.cod_segmento_gest ,
	RE.cod_gestor ,
	RE.cod_univ ,
	IF(RE.cod_gestor_prod = 'null', cast(NULL as string), RE.cod_gestor_prod) cod_gestor_prod ,
	RE.cod_origen_inf , 
	(sum(COALESCE(RE.IMP_AJTTI_EGR_TB_CAP_ML, 0.0))
	+ sum(COALESCE(RE.IMP_AJTTI_EGR_SL_CAP_ML, 0.0))
	+ sum(COALESCE(RE.IMP_AJTTI_EGR_PER_ML, 0.0))
	+ sum(COALESCE(RE. IMP_AJTTI_EGR_REAJUSTE_ML, 0.0))
	+ sum(COALESCE(RE.IMP_AJTTI_ING_TB_CAP_ML, 0.0))
	+ sum(COALESCE(RE.IMP_AJTTI_ING_SL_CAP_ML, 0.0))
	+ sum(COALESCE(RE.IMP_AJTTI_ING_PER_ML, 0.0))
	+ sum(COALESCE(RE.IMP_AJTTI_ING_REAJUSTE_ML, 0.0))
	+ sum(COALESCE(RE.IMP_EFEC_ENC_ML, 0.0))) AS fc_ren_out_tti , -- importe por tasa 202012
	sum(COALESCE(RE.IMP_ING_PER_ML, 0.0)) AS fc_ren_imp_ing_per_ml ,
	sum(COALESCE(RE.IMP_ING_CAP_ML, 0.0)) AS fc_ren_imp_ing_cap_ml ,
	sum(COALESCE(RE.IMP_ING_REAJ_ML, 0.0)) AS fc_ren_ing_reaj_ml ,
	sum(COALESCE(RE.IMP_EGR_PER_ML, 0.0)) AS fc_ren_egr_per_ml ,
	sum(COALESCE(RE.IMP_EGR_CAP_ML, 0.0)) AS fc_ren_egr_cap_ml ,
	sum(COALESCE(RE.IMP_EGR_REAJ_ML, 0.0)) AS fc_ren_egr_reaj_ml ,
	sum(COALESCE(RE.IMP_AJTTI_EGR_TB_CAP_ML, 0.0)) AS fc_ren_ajtti_egr_tb_cap_ml ,
	sum(COALESCE(RE.IMP_AJTTI_EGR_SL_CAP_ML, 0.0)) AS fc_ren_ajtti_egr_sl_cap_ml ,
	sum(COALESCE(RE.IMP_AJTTI_EGR_PER_ML, 0.0)) AS fc_ren_ajtti_egr_per_ml ,
	sum(COALESCE(RE.IMP_AJTTI_EGR_REAJUSTE_ML, 0.0)) AS fc_ren_ajtti_egr_reajuste_ml ,
	sum(COALESCE(RE.IMP_AJTTI_ING_TB_CAP_ML, 0.0)) AS fc_ren_ajtti_ing_tb_cap_ml ,
	sum(COALESCE(RE.IMP_AJTTI_ING_SL_CAP_ML, 0.0)) AS fc_ren_ajtti_ing_sl_cap_ml ,
	sum(COALESCE(RE.IMP_AJTTI_ING_PER_ML, 0.0)) AS fc_ren_ajtti_ing_per_ml ,
	sum(COALESCE(RE.IMP_AJTTI_ING_REAJUSTE_ML, 0.0)) AS fc_ren_ajtti_ing_reajuste_ml ,
	sum(COALESCE(RE.IMP_EFEC_ENC_ML, 0.0)) AS fc_ren_efec_enc_ml ,
	sum(COALESCE(RE.IMP_ING_PER_MO, 0.0)) AS fc_ren_ing_per_mo ,
	sum(COALESCE(RE.IMP_ING_CAP_MO, 0.0)) AS fc_ren_ing_cap_mo ,
	sum(COALESCE(RE.IMP_ING_REAJ_MO, 0.0)) AS fc_ren_ing_reaj_mo ,
	sum(COALESCE(RE.IMP_EGR_PER_MO, 0.0)) AS fc_ren_egr_per_mo ,
	sum(COALESCE(RE.IMP_EGR_CAP_MO, 0.0)) AS fc_ren_egr_cap_mo ,
	sum(COALESCE(RE.IMP_EGR_REAJ_MO, 0.0)) AS fc_ren_egr_reaj_mo ,
	sum(COALESCE(RE.IMP_AJTTI_EGR_TB_CAP_MO, 0.0)) AS fc_ren_ajtti_egr_tb_cap_mo ,
	sum(COALESCE(RE.IMP_AJTTI_EGR_SL_CAP_MO, 0.0)) AS fc_ren_ajtti_egr_sl_cap_mo ,
	sum(COALESCE(RE.IMP_AJTTI_EGR_PER_MO, 0.0)) AS fc_ren_ajtti_egr_per_mo ,
	sum(COALESCE(RE.IMP_AJTTI_EGR_REAJUSTE_MO, 0.0)) AS fc_ren_ajtti_egr_reajuste_mo ,
	sum(COALESCE(RE.IMP_AJTTI_ING_TB_CAP_MO, 0.0)) AS fc_ren_ajtti_ing_tb_cap_mo ,
	sum(COALESCE(RE.IMP_AJTTI_ING_SL_CAP_MO, 0.0)) AS fc_ren_ajtti_ing_sl_cap_mo ,
	sum(COALESCE(RE.IMP_AJTTI_ING_PER_MO, 0.0)) AS fc_ren_ajtti_ing_per_mo ,
	sum(COALESCE(RE.IMP_AJTTI_ING_REAJUSTE_MO, 0.0)) AS fc_ren_ajtti_ing_reajuste_mo ,
	sum(COALESCE(RE.IMP_EFEC_ENC_MO, 0.0)) AS fc_ren_efec_enc_mo ,
	sum(COALESCE(RE.IMP_SDO_CAP_MED_ML, 0.0)) AS fc_ren_sdo_cap_med_ml ,
	sum(COALESCE(RE.IMP_SDO_MED_INT_ML, 0.0)) AS fc_ren_sdo_med_int_ml ,
	sum(COALESCE(RE.IMP_SDO_MED_REAJUSTE_ML, 0.0)) AS fc_ren_sdo_med_reajuste_ml ,
	sum(COALESCE(RE.IMP_SDO_MED_INSOLV_ML, 0.0)) AS fc_ren_sdo_med_insolv_ml ,
	sum(COALESCE(RE.IMP_SDO_CAP_MED_MO, 0.0)) AS fc_ren_sdo_cap_med_mo ,
	sum(COALESCE(RE.IMP_SDO_MED_INT_MO, 0.0)) AS fc_ren_sdo_med_int_mo ,
	sum(COALESCE(RE.IMP_SDO_MED_REAJUSTE_MO, 0.0)) AS fc_ren_sdo_med_reajuste_mo ,
	sum(COALESCE(RE.IMP_SDO_MED_INSOLV_MO, 0.0)) AS fc_ren_sdo_med_insolv_mo ,
	sum(COALESCE(RE.IMP_ING_PER_ML, 0.0)) AS fc_ren_ing_per_ml ,
	(sum(COALESCE(RE.IMP_ING_PER_ML, 0.0))
	+ sum(COALESCE(RE.IMP_ING_CAP_ML, 0.0))
	+ sum(COALESCE(RE.IMP_ING_REAJ_ML, 0.0))
	+ sum(COALESCE(RE.IMP_EGR_PER_ML, 0.0))
	+ sum(COALESCE(RE.IMP_EGR_CAP_ML, 0.0))
	+ sum(COALESCE(RE.IMP_EGR_REAJ_ML, 0.0))
	+ sum(COALESCE(RE.IMP_AJTTI_EGR_TB_CAP_ML, 0.0))
	+ sum(COALESCE(RE.IMP_AJTTI_EGR_SL_CAP_ML, 0.0))
	+ sum(COALESCE(RE.IMP_AJTTI_EGR_PER_ML, 0.0))
	+ sum(COALESCE(RE.IMP_AJTTI_EGR_REAJUSTE_ML, 0.0))
	+ sum(COALESCE(RE.IMP_AJTTI_ING_TB_CAP_ML, 0.0))
	+ sum(COALESCE(RE.IMP_AJTTI_ING_SL_CAP_ML, 0.0))
	+ sum(COALESCE(RE.IMP_AJTTI_ING_PER_ML, 0.0))
	+ sum(COALESCE(RE.IMP_AJTTI_ING_REAJUSTE_ML, 0.0))
	+ sum(COALESCE(RE.IMP_EFEC_ENC_ML, 0.0))) AS fc_ren_resultado_total_ML ,
	(sum(COALESCE(RE.IMP_ING_PER_MO, 0.0))
	+ sum(COALESCE(RE.IMP_ING_CAP_MO, 0.0))
	+ sum(COALESCE(RE.IMP_ING_REAJ_MO, 0.0))
	+ sum(COALESCE(RE.IMP_EGR_PER_MO, 0.0))
	+ sum(COALESCE(RE.IMP_EGR_CAP_ML, 0.0))
	+ sum(COALESCE(RE.IMP_EGR_REAJ_MO, 0.0))
	+ sum(COALESCE(RE.IMP_AJTTI_EGR_TB_CAP_MO, 0.0))
	+ sum(COALESCE(RE.IMP_AJTTI_EGR_SL_CAP_MO, 0.0))
	+ sum(COALESCE(RE.IMP_AJTTI_EGR_PER_MO, 0.0))
	+ sum(COALESCE(RE.IMP_AJTTI_EGR_REAJUSTE_MO, 0.0))
	+ sum(COALESCE(RE.IMP_AJTTI_ING_TB_CAP_MO, 0.0))
	+ sum(COALESCE(RE.IMP_AJTTI_ING_SL_CAP_MO, 0.0))
	+ sum(COALESCE(RE.IMP_AJTTI_ING_PER_MO, 0.0))
	+ sum(COALESCE(RE.IMP_AJTTI_ING_REAJUSTE_MO, 0.0))
	+ sum(COALESCE(RE.IMP_EFEC_ENC_MO, 0.0))) AS fc_ren_resultado_total_MO ,
	(sum(COALESCE(RE.IMP_SDO_CAP_MED_ML, 0.0))
	+ sum(COALESCE(RE.IMP_SDO_MED_INT_ML, 0.0))
	+ sum(COALESCE(RE.IMP_SDO_MED_REAJUSTE_ML, 0.0))
	+ sum(COALESCE(RE.IMP_SDO_MED_INSOLV_ML, 0.0))) AS fc_ren_saldo_medio_total_ML ,
	(sum(COALESCE(RE.IMP_SDO_CAP_MED_MO, 0.0))
	+ sum(COALESCE(RE.IMP_SDO_MED_INT_MO, 0.0))
	+ sum(COALESCE(RE.IMP_SDO_MED_REAJUSTE_MO, 0.0))
	+ sum(COALESCE(RE.IMP_SDO_MED_INSOLV_MO, 0.0))) AS fc_ren_saldo_medio_total_MO ,
	(sum(COALESCE(RE.IMP_ING_PER_ML, 0.0))
	+ sum(COALESCE(RE.IMP_ING_CAP_ML, 0.0))
	+ sum(COALESCE(RE.IMP_ING_REAJ_ML, 0.0))
	+ sum(COALESCE(RE.IMP_EGR_PER_ML, 0.0))
	+ sum(COALESCE(RE.IMP_EGR_CAP_ML, 0.0))
	+ sum(COALESCE(RE.IMP_EGR_REAJ_ML, 0.0))) AS fc_ren_resultado_total_real_ML ,
	(sum(COALESCE(RE.IMP_ING_PER_MO, 0.0))
	+ sum(COALESCE(RE.IMP_ING_CAP_MO, 0.0))
	+ sum(COALESCE(RE.IMP_ING_REAJ_MO, 0.0))
	+ sum(COALESCE(RE.IMP_EGR_PER_MO, 0.0))
	+ sum(COALESCE(RE.IMP_EGR_CAP_MO, 0.0))
	+ sum(COALESCE(RE.IMP_EGR_REAJ_MO, 0.0))) AS fc_ren_resultado_total_real_MO ,
	(sum(COALESCE(RE.IMP_SDO_CAP_ML, 0.0))
	+ sum(COALESCE(RE.IMP_SDO_INT_ML, 0.0))
	+ sum(COALESCE(RE.IMP_SDO_INSOLV_ML, 0.0))
	+ sum(COALESCE(RE.IMP_SDO_REAJUSTE_ML, 0.0))) AS fc_ren_saldo_puntual_ML ,
	(sum(COALESCE(RE.IMP_SDO_CAP_MO, 0.0))
	+ sum(COALESCE(RE.IMP_SDO_INT_MO, 0.0))
	+ sum(COALESCE(RE.IMP_SDO_INSOLV_MO, 0.0))
	+ sum(COALESCE(RE.IMP_SDO_REAJUSTE_MO, 0.0))) AS fc_ren_saldo_puntual_MO ,
	RE.cod_sis_origen ,
	CONCAT(TRIM(RE.cod_tip_ajuste),TRIM(RE.cod_origen_inf)) internegocio ,
	RE.partition_date 
FROM bi_corp_staging.rio157_ms0_ft_dwh_blce_result RE 
WHERE RE.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_to', dag_id='LOAD_CMN-RORAC_inputs') }}' 
	--AND RE.ind_pool = 'N'
	--AND (RE.imp_sdo_cap_ml != '0' OR RE.imp_sdo_cap_mo != '0' OR RE.imp_sdo_int_ml != '0'
	--	OR RE.imp_sdo_int_mo != '0' OR RE.imp_sdo_reajuste_ml != '0' OR RE.imp_sdo_reajuste_mo != '0'
	--	OR RE.imp_sdo_insolv_ml	!= '0' OR RE.imp_sdo_insolv_mo != '0' OR RE.imp_sdo_cap_med_ml != '0'
	--	OR RE.imp_sdo_cap_med_mo != '0' OR RE.imp_sdo_med_int_ml != '0' OR RE.imp_sdo_med_int_mo != '0'
	--	OR RE.imp_sdo_med_reajuste_ml != '0' OR RE.imp_sdo_med_reajuste_mo != '0' OR RE.imp_sdo_med_insolv_ml != '0'
	--	OR RE.imp_sdo_med_insolv_mo	!= '0' OR RE.imp_egr_cap_ml	!= '0' OR RE.imp_egr_cap_mo	!= '0'
	--	OR RE.imp_egr_per_ml != '0' OR RE.imp_egr_per_mo != '0' OR RE.imp_egr_reaj_ml != '0'
	--	OR RE.imp_egr_reaj_mo != '0' OR RE.imp_ing_cap_ml != '0' OR RE.imp_ing_cap_mo != '0'
	--	OR RE.imp_ing_per_ml != '0' OR RE.imp_ing_per_mo != '0' OR RE.imp_ing_reaj_ml != '0'
	--	OR RE.imp_ing_reaj_mo != '0' OR RE.imp_ajtti_egr_tb_cap_ml != '0' OR RE.imp_ajtti_egr_tb_cap_mo != '0'
	--	OR RE.imp_ajtti_egr_sl_cap_ml != '0' OR RE.imp_ajtti_egr_sl_cap_mo != '0' OR RE.imp_ajtti_egr_per_ml != '0'
	--	OR RE.imp_ajtti_egr_per_mo != '0' OR RE.imp_ajtti_egr_reajuste_ml != '0' OR RE.imp_ajtti_egr_reajuste_mo != '0'
	--	OR RE.imp_ajtti_ing_tb_cap_ml != '0' OR RE.imp_ajtti_ing_tb_cap_mo != '0' OR RE.imp_ajtti_ing_sl_cap_ml != '0'
	--	OR RE.imp_ajtti_ing_sl_cap_mo != '0' OR RE.imp_ajtti_ing_per_ml	!= '0' OR RE.imp_ajtti_ing_per_mo != '0'
	--	OR RE.imp_ajtti_ing_reajuste_ml	!= '0' OR RE.imp_ajtti_ing_reajuste_mo != '0' OR RE.imp_efec_enc_ml != '0'
	--	OR RE.imp_efec_enc_mo != '0')
	AND RE.cod_contenido IN ('PRE','CRE','ARF','CCO','COM','CTB','ROF') 
	OR (RE.cod_contenido = 'CCL' AND SUBSTRING(RE.cod_producto_gest, 1, 2) != 'C-') -- lógica para ccl 202012
GROUP BY CONCAT(RE.cod_contenido, RE.idf_cto_ods) ,
	CONCAT(RE.cod_contenido,RE.idf_pers_ods) ,
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
	RE.cod_segmento_gest ,
	RE.cod_gestor ,
	RE.cod_univ ,
	RE.cod_gestor_prod ,
	RE.cod_origen_inf ,
	RE.partition_date ,
	-------------------------
	COALESCE(RE.OUT_TTI, 0) ,
	RE.cod_sis_origen ,
	CONCAT(TRIM(RE.cod_tip_ajuste),TRIM(RE.cod_origen_inf)) ;

-------------------------------------------------------------------------
-- ACTIVO_RESULT_G 
CREATE TEMPORARY TABLE activo_result_g AS
SELECT RE.*
	, gen.cod_ren_producto_generic
	, gen.cod_ren_subproducto_generic
	, gen.dt_ren_altacontrato
	, gen.dt_ren_vencontrato
	, gen.dt_ren_reestruccontrato
FROM activo_result RE
LEFT JOIN bi_corp_common.dim_ren_genericresult gen ON 
	RE.idf_cto_ods = gen.cod_ren_contrato
	AND RE.cod_contenido = gen.cod_ren_contenido
WHERE gen.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_to', dag_id='LOAD_CMN-RORAC_inputs') }}'
AND gen.cod_ren_contenido IN ('PRE','CRE','ARF','CCO','COM','CCL','CTB','ROF') ; 

-------------------------------------------------------------------------
-- ACTIVO_RESULT_GC DROP TABLE activo_result_gc
CREATE TEMPORARY TABLE activo_result_gc AS
SELECT RE.*	
	, cli.cod_ren_vincula
	, cli.cod_per_tipopersona
	, cli.ds_per_nombre_apellido
FROM activo_result_g RE
LEFT JOIN bi_corp_common.dim_ren_clienteresult cli ON
	cli.idf_pers_ods = RE.idf_pers_ods 
WHERE cli.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_to', dag_id='LOAD_CMN-RORAC_inputs') }}' ;

-------------------------------------------------------------------------
-- ~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^~^
-- PRODUCTO_MIS -- DROP TABLE producto_mis
CREATE TEMPORARY TABLE producto_mis AS
SELECT TRIM(cod_valor) cod_producto_gest
	, TRIM(cod_valor_padre) cod_producto_mis -- 20201013
	, TRIM(num_nivel) num_nivel
FROM bi_corp_staging.rio157_ms0_dm_dwh_composicion_jerqs 
WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_rio157_ms0_dm_dwh_composicion_jerqs', dag_id='LOAD_CMN-RORAC_inputs') }}'
	AND cod_jerarquia = 'JBP01'
	AND cod_dimension = 'PR01' ;

-------------------------------------------------------------------------
-- RESULT_ACTIVO (+ JERARQUIAS)
CREATE TEMPORARY TABLE result_activo AS
SELECT RE.* ,
	adn2.cod_ren_area_negocio_niv_2 id_division , -- 20201013 -- OK
	adn.cod_ren_primercorporativo , -- 20201013 -- OK
	pm.cod_producto_mis , -- 20201013 -- OK 
	adn2.cod_ren_area_negocio_niv_9 ,
	prod.cod_ren_producto_niv_3 ,
	prod.cod_ren_producto_niv_4 ,
	cta.cod_ren_cta_resultados_niv_6 ,
	cta.cod_ren_cta_resultados_niv_8 ,
	cta.cod_ren_cta_resultados_niv_9 
FROM activo_result_gc RE
---------------------------------------------------------------
LEFT JOIN bi_corp_common.dim_ren_productosctrldn prod ON
	RE.cod_producto_gest = prod.cod_ren_producto 
	AND prod.cod_ren_producto_niv_3 IN ('BG1','BG3')
---------------------------------------------------------------
LEFT JOIN bi_corp_common.dim_ren_areanegocioctr adn2 ON 
	RE.cod_area_negocio = adn2.cod_ren_area_negocio_niv_9 
----------------------------------------------------------------	
LEFT JOIN bi_corp_common.dim_ren_jeareanegocioctr adn ON 
	RE.cod_area_negocio = adn.cod_ren_areanegociohijo
----------------------------------------------------------------	
LEFT JOIN producto_mis pm ON 
	RE.cod_producto_gest = PM.cod_producto_gest
---------------------------------------------------------------
LEFT JOIN bi_corp_staging.rio157_ms0_dm_je_dwh_entidades_ctr entj ON 
	entj.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_rio157_ms0_dm_je_dwh_entidades_ctr entj', dag_id='LOAD_CMN-RORAC_inputs') }}'
	AND RE.cod_pais = entj.cod_pais 
	AND entj.cod_hijo = '10001'
---------------------------------------------------------------
LEFT JOIN bi_corp_common.dim_ren_ctaresultadosctr cta ON
	RE.cod_cta_cont_gestion  = cta.cod_ren_cta_resultados
	AND cta.cod_ren_cta_resultados_niv_6 != '1073'
---------------------------------------------------------------
WHERE RE.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_to', dag_id='LOAD_CMN-RORAC_inputs') }}' ;

-------------------------------------------------------------------------
-- RESULT_ACTIVO_I DROP TABLE result_activo_i
CREATE TEMPORARY TABLE result_activo_i AS
SELECT 
	'ARG' AS cod_ren_unidad	 ,
	date_format(partition_date, 'yyyyMM') dt_ren_fec_data ,
	1 AS cod_ren_finalidad_datos ,
	cod_ren_contrato_rorac , 
	cod_ren_cliente_rorac ,
	idf_cto_ods AS cod_ren_contrato	 ,
	cod_area_negocio ,
	'ARS' AS  cod_ren_divisa ,
	id_division AS cod_ren_division , 
	cod_producto_mis , -- producto_generic
	idf_pers_ods ,
	CASE WHEN cod_ren_producto_niv_3 = 'BG1'THEN SUM(fc_ren_resultado_total_ml) ELSE 0 END AS fc_ren_margenmes ,
	CASE WHEN cod_ren_producto_niv_3 = 'BG1'THEN SUM(fc_ren_saldo_medio_total_ml) ELSE 0 END AS fc_ren_sdbsmv ,
	CASE WHEN cod_ren_producto_niv_3 = 'BG3' THEN (SUM(fc_ren_sdo_cap_med_ml)
										+ SUM(fc_ren_sdo_med_int_ml)
										+ SUM(fc_ren_sdo_med_reajuste_ml)
										+ SUM(fc_ren_sdo_med_insolv_ml)) ELSE 0 END AS fc_ren_sfbsmv , 
	date_format(dt_ren_altacontrato, 'dd-MM-yyyy') dt_ren_altacontrato ,
	date_format(dt_ren_vencontrato, 'dd-MM-yyyy') dt_ren_vencontrato ,
	cod_entidad_espana	 ,
	cod_centro_cont ,
	cod_ren_subproducto_generic , --generic
    cast(NULL as string) AS zona ,
    cast(NULL as string) AS territorial,
	cod_gestor_prod	 ,
	cod_gestor	 ,
	CASE WHEN cod_ren_producto_niv_3 = 'BG1' THEN (SUM(fc_ren_imp_ing_per_ml)
										+ SUM(fc_ren_imp_ing_cap_ml)
										+ SUM(fc_ren_sdo_cap_med_ml)
										+ SUM(fc_ren_sdo_med_int_ml)
										+ SUM(fc_ren_sdo_med_reajuste_ml)
										+ SUM(fc_ren_sdo_med_insolv_ml)) ELSE 0 END AS fc_ren_sdbsfm ,
	CASE WHEN cod_ren_producto_niv_3 = 'BG1' AND cod_ren_cta_resultados_niv_9 = '1031' THEN SUM(fc_ren_resultado_total_real_ml) ELSE 0 END AS fc_ren_interes ,
	CASE WHEN cod_ren_producto_niv_3 = 'BG1' AND cod_ren_cta_resultados_niv_9 = '1032' THEN SUM(fc_ren_resultado_total_real_ml) ELSE 0 END AS fc_ren_comfin ,
	CASE WHEN cod_ren_producto_niv_3 = 'BG1' AND cod_ren_cta_resultados_niv_8 = '1051' THEN SUM(fc_ren_resultado_total_real_ml) ELSE 0 END AS fc_ren_comnofin ,
	cod_ren_vincula ,
	CASE WHEN cod_ren_producto_niv_3 = 'BG1' AND cod_ren_cta_resultados_niv_8 = '1052' THEN SUM(fc_ren_resultado_total_real_ml) ELSE 0 END AS fc_ren_rof ,
	fc_ren_out_tti AS fc_ren_tti , -- OUT_TTI blce_result --
    cast(NULL as string) AS  mtm ,
    cast(NULL as string) AS  nocional ,
    cod_divisa ,
    cast(NULL as int) AS flag_mora_local ,
    cast(NULL as int) AS flag_carterizadas ,
	ds_per_nombre_apellido ,
	cod_per_tipopersona ,
    cast(NULL as string) AS nifcif ,
    cod_origen_inf AS intragrupo ,
	internegocio AS cod_ren_internegocio , -- (resolver desde blce result) -- 
    cod_ren_primercorporativo AS area_negocio_corp , -- 20201001
	cod_producto_gest ,
	cod_segmento_gest ,
    cod_ren_producto_generic AS cod_producto_operacional ,
	cast(NULL as string) AS ds_ren_ficheromis ,
	date_format(dt_ren_reestruccontrato, 'dd-MM-yyyy') dt_ren_reestruccontrato ,
	date_format(partition_date, 'dd-MM-yyyy') dt_ren_fec_extr_datos ,
    IF(SUBSTRING(cod_ren_contrato_rorac,1,3) = ('CCL'), 2, 0) flagneteo ,
	CASE WHEN cod_ren_producto_niv_3 = 'BG1' AND cod_ren_cta_resultados_niv_8 = '1054' THEN SUM(fc_ren_resultado_total_real_ml) ELSE 0 END AS fc_ren_orex ,
	cast(NULL as decimal(20,4)) AS fc_ren_costemensualcto ,
	ROW_NUMBER() OVER(PARTITION BY SUBSTRING(idf_cto_ods,1,26)) order_contrato ,
	partition_date
FROM result_activo 
WHERE cod_ren_cta_resultados_niv_6 != '1073'
GROUP BY --fec_data ,
		cod_ren_contrato_rorac ,
		cod_ren_cliente_rorac ,
		idf_cto_ods ,
		cod_area_negocio ,
		id_division ,
		cod_producto_mis ,
		idf_pers_ods ,
		dt_ren_altacontrato	 ,
		dt_ren_vencontrato	 ,
		cod_entidad_espana	 ,
		cod_centro_cont ,
		cod_ren_subproducto_generic ,
	    cod_gestor_prod	 ,
		cod_gestor	 ,
		cod_ren_vincula ,
		fc_ren_out_tti ,
		cod_divisa ,
	    ds_per_nombre_apellido ,
		cod_per_tipopersona ,
	    cod_origen_inf ,
		cod_ren_primercorporativo , 
		cod_producto_gest ,
		cod_segmento_gest ,
	    dt_ren_reestruccontrato ,
		partition_date ,
	    cod_ren_producto_niv_3 ,
		cod_ren_producto_niv_4 ,
		cod_ren_cta_resultados_niv_8 , 
		cod_ren_cta_resultados_niv_9 ,
		cod_ren_producto_generic ,
		internegocio ;

-------------------------------------------------------------------------
-- TABLON_IFRS9 (s_provision_amount)
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
WHERE RI.periodo = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day_arda', dag_id='LOAD_CMN-RORAC_inputs') }}'
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

-------------------------------------------------------------------------
-- ROSETTA_KEY (id_cto_bdr)
CREATE TEMPORARY TABLE IF NOT EXISTS rosetta_key AS 
SELECT MK.native_key AS cod_ren_contrato_rorac
	 , BDR.native_key AS id_cto_bdr
FROM bi_corp_common.rosetta_nkey_hist MK
	JOIN bi_corp_common.rosetta_nkey_hist BDR ON 
		MK.master_key = BDR.master_key 
		AND BDR.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_to', dag_id='LOAD_CMN-RORAC_inputs') }}'
		AND BDR.domain_code = '00004'
WHERE MK.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_to', dag_id='LOAD_CMN-RORAC_inputs') }}'
	AND MK.domain_code = '00005';

-------------------------------------------------------------------------
-- INSERT COMMON TABLE
INSERT OVERWRITE TABLE bi_corp_common.bt_ror_input_activo
PARTITION(partition_date) 
SELECT RE.cod_ren_unidad ,
	RE.dt_ren_fec_data ,
	RE.cod_ren_finalidad_datos ,
	IF(SUBSTRING(RE.cod_ren_contrato_rorac,1,3) IN ('CCL','CTB'), RE.cod_ren_cliente_rorac, RE.cod_ren_contrato_rorac) cod_ren_contrato_rorac ,
	RE.cod_ren_contrato ,
	RE.cod_area_negocio ,
	RE.cod_ren_divisa ,
	RE.cod_ren_division ,
	RE.cod_producto_mis ,
	RE.idf_pers_ods ,
	SUM(RE.fc_ren_margenmes) ,
	SUM(RE.fc_ren_sdbsmv) ,
	SUM(RE.fc_ren_sfbsmv) ,
	RE.dt_ren_altacontrato ,
	IF(RE.dt_ren_vencontrato IS NULL, '01-01-9999', RE.dt_ren_vencontrat) ,
	RE.cod_entidad_espana ,
	RE.cod_centro_cont ,
	RE.cod_ren_subproducto_generic ,
	RE.zona ,
	RE.territorial ,
	RE.cod_gestor_prod ,
	RE.cod_gestor ,
	RK.id_cto_bdr ,
	SUM(RE.fc_ren_sdbsfm) ,
	SUM(RE.fc_ren_interes) ,
	SUM(RE.fc_ren_comfin) ,
	SUM(RE.fc_ren_comnofin) ,
	IF(RE.cod_ren_vincula = 'null', NULL, RE.cod_ren_vincula) ,
	SUM(RE.fc_ren_rof) ,
	MAX(RE.fc_ren_tti) ,
	RE.mtm ,
	RE.nocional ,
	RE.cod_divisa ,
	RE.flag_mora_local ,
	RE.flag_carterizadas ,
	RE.ds_per_nombre_apellido ,
	IF(RE.cod_per_tipopersona = 'null', NULL, RE.cod_per_tipopersona) ,
	RE.nifcif ,
	RE.intragrupo ,
	RE.cod_ren_internegocio ,
	RE.area_negocio_corp ,
	RE.cod_producto_gest ,
	RE.cod_segmento_gest ,
	RE.cod_producto_operacional ,
	CONCAT('{{ ti.xcom_pull(task_ids='InputConfig', key='last_calendar_day_arda', dag_id='LOAD_CMN-RORAC_inputs') }}','v1') ds_ren_ficheromis ,
	RE.dt_ren_reestruccontrato ,
	RE.dt_ren_fec_extr_datos ,
	RE.flagneteo ,
	SUM(RE.fc_ren_orex) ,
	SUM(RI.s_provision_amount) ,
	RE.fc_ren_costemensualcto ,
    IF(SUBSTRING(RE.cod_ren_contrato_rorac,1,3) NOT IN ('CCL','CTB'), 0, IF(LENGTH(RE.cod_ren_cliente_rorac) > 9, 1, 3)) cod_nivel_operacion ,
	RE.partition_date
FROM result_activo_i RE
LEFT JOIN tablon_ifrs9 RI ON 
	SUBSTRING(RE.cod_ren_contrato,1,26) = RI.cod_ren_contrato_short
	AND RE.order_contrato = 1 
LEFT JOIN rosetta_key RK ON 
	RE.cod_ren_contrato_rorac = RK.cod_ren_contrato_rorac 
GROUP BY RE.cod_ren_unidad ,
		RE.dt_ren_fec_data ,
		RE.cod_ren_finalidad_datos ,
		IF(SUBSTRING(RE.cod_ren_contrato_rorac,1,3) IN ('CCL','CTB'), RE.cod_ren_cliente_rorac, RE.cod_ren_contrato_rorac) ,
		RE.cod_ren_contrato ,
		RE.cod_area_negocio ,
		RE.cod_ren_divisa ,
		RE.cod_ren_division ,
		RE.cod_producto_mis ,
		RE.idf_pers_ods ,
		RE.dt_ren_altacontrato ,
		IF(RE.dt_ren_vencontrato IS NULL, '01-01-9999', RE.dt_ren_vencontrat) ,
		RE.cod_entidad_espana ,
		RE.cod_centro_cont ,
		RE.cod_ren_subproducto_generic ,
		RE.zona ,
		RE.territorial ,
		RE.cod_gestor_prod ,
		RE.cod_gestor ,
		RK.id_cto_bdr ,
		IF(RE.cod_ren_vincula = 'null', NULL, RE.cod_ren_vincula) ,
		RE.mtm ,
		RE.nocional ,
		RE.cod_divisa ,
		RE.flag_mora_local ,
		RE.flag_carterizadas ,
		RE.ds_per_nombre_apellido ,
		IF(RE.cod_per_tipopersona = 'null', NULL, RE.cod_per_tipopersona) ,
		RE.nifcif ,
		RE.intragrupo ,
		RE.cod_ren_internegocio ,
		RE.area_negocio_corp ,
		RE.cod_producto_gest ,
		RE.cod_segmento_gest ,
		RE.cod_producto_operacional ,
		RE.dt_ren_reestruccontrato ,
		RE.dt_ren_fec_extr_datos ,
		RE.fc_ren_costemensualcto ,
		RE.flagneteo ,
		IF(SUBSTRING(RE.cod_ren_contrato_rorac,1,3) NOT IN ('CCL','CTB'), 0, IF(LENGTH(RE.cod_ren_cliente_rorac) > 9, 1, 3)) ,
		RE.partition_date ;