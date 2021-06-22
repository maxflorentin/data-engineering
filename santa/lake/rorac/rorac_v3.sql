--                                         ___ _____
--     _________  _________ ______   _   _<  /|__  /
--    / ___/ __ \/ ___/ __ `/ ___/  | | / / /  /_ < 
--   / /  / /_/ / /  / /_/ / /__    | |/ / / ___/ / 
--  /_/   \____/_/   \__,_/\___/    |___/_(_)____/  
--  
----------------------------------------------------------------------------------------------
---------------------------------------------------------------------- CONTRATOS POR CONTENIDO
set hive.execution.engine=spark;
set spark.yarn.queue=root.dataeng;
---------------------------------------
CREATE TEMPORARY TABLE cto_contenido AS
WITH 

cto_act_pas AS (

	SELECT DISTINCT RE.cod_ren_contrato 
			, RE.cod_ren_contenido 
			, RE.cod_ren_contrato_rorac 
			, IF(RE.cod_ren_contenido IN ('PLZ','CTA'), 'P', IF(RE.cod_ren_contenido IN ('PRE','CRE','ARF','CCO'), 'A', NULL)) perimetro
	FROM bi_corp_common.bt_ren_result RE
	WHERE RE.partition_date = '2020-10-31'
	AND RE.cod_ren_contenido IN ('PRE','CRE','ARF','CCO','PLZ','CTA','COM','CTB','ROF','CCL')
	AND RE.cod_ren_contrato != '-99100'
	ORDER BY RE.cod_ren_contrato , cod_ren_contenido , RE.cod_ren_contrato_rorac 
), 

cto_act_pas_join AS (

	SELECT A.cod_ren_contrato , A.cod_ren_contenido , A.cod_ren_contrato_rorac  , IF(A.perimetro IS NULL, B.perimetro, A.perimetro) perimetro
	FROM cto_act_pas A
		LEFT JOIN cto_act_pas B 
			ON A.cod_ren_contrato = B.cod_ren_contrato
			AND B.perimetro IS NOT NULL 
)

	SELECT cod_ren_contrato , cod_ren_contenido , cod_ren_contrato_rorac  , perimetro
	FROM cto_act_pas_join WHERE perimetro IS NOT NULL ;

---------------------------------------------------------------------- CONTRATOS POR CONTENIDO A/P
CREATE TEMPORARY TABLE cto_contenido_a AS
	SELECT cod_ren_contrato , cod_ren_contenido , cod_ren_contrato_rorac 
	FROM cto_contenido WHERE perimetro = 'A' ;

CREATE TEMPORARY TABLE cto_contenido_p AS
	SELECT cod_ren_contrato , cod_ren_contenido , cod_ren_contrato_rorac 
	FROM cto_contenido WHERE perimetro = 'P' ;

---------------------------------------------------------------------------------------------
---------------------------------------------------------------------- CONTRATOS POR PRODUCTO
set hive.execution.engine=spark;
set spark.yarn.queue=root.dataeng;
-----------------------------------------
CREATE TEMPORARY TABLE cto_producto AS
WITH
pro_act_pas AS (

	SELECT DISTINCT RE.cod_ren_contrato 
			, RE.cod_ren_contenido 
			, RE.cod_ren_contrato_rorac 
			, IF(SUBSTRING(RE.cod_ren_producto_gest, 1, 2) = 'C+', 'A', 'P') perimetro
			, RE.cod_ren_producto_gest
	FROM bi_corp_common.bt_ren_result RE
	WHERE RE.partition_date = '2020-10-31'
	AND RE.cod_ren_contenido IN ('COM','CTB','ROF','CCL')
	AND RE.cod_ren_contrato = '-99100'
	AND SUBSTRING(RE.cod_ren_producto_gest, 1, 2) IN ('C+','C-')
)

	SELECT cod_ren_contrato , cod_ren_contenido , cod_ren_contrato_rorac  , perimetro , cod_ren_producto_gest
	FROM pro_act_pas  ;

---------------------------------------------------------------------- CONTRATOS POR PRODUCTO A/P
CREATE TEMPORARY TABLE cto_producto_a AS
	SELECT cod_ren_contrato , cod_ren_contenido , cod_ren_contrato_rorac , cod_ren_producto_gest
	FROM cto_producto WHERE perimetro = 'A' ;

CREATE TEMPORARY TABLE cto_producto_p AS
	SELECT cod_ren_contrato , cod_ren_contenido , cod_ren_contrato_rorac , cod_ren_producto_gest
	FROM cto_producto WHERE perimetro = 'P' ;

----------------------------------------------------------------------------------------------
----------------------------------------------------------------------- RESULT JOIN PERIMETROS
set hive.execution.engine=spark;
set spark.yarn.queue=root.dataeng;
----------------------------------------- ACTIVO
CREATE TEMPORARY TABLE cto_activo AS
	SELECT RE.cod_ren_contrato
		, RE.cod_ren_cliente_rorac
		, RE.cod_ren_contrato_rorac
		, RE.cod_ren_pers_ods
		, RE.cod_ren_contenido
		, RE.cod_ren_area_negocio
		, RE.cod_ren_producto_niv_3
		, RE.dt_ren_altacontrato 
		, RE.dt_ren_vencontrato 
		, RE.cod_ren_entidad_espana 
		, RE.cod_ren_centro_cont cod_ren_centrocont
		, IF(SUBSTRING(RE.cod_ren_contrato,11,4) = '', NULL, SUBSTRING(RE.cod_ren_contrato,11,4)) cod_ren_subprodu
		, RE.cod_ren_gestor_prod 
		, IF(RE.cod_ren_gestor = 'null', NULL, RE.cod_ren_gestor) cod_ren_gestor 
		, RE.cod_ren_cta_resultados_niv_9
		, RE.cod_ren_cta_resultados_niv_8
		, IF(RE.cod_ren_vincula = 'null', 0, RE.cod_ren_vincula) cod_ren_vincula
		, RE.cod_ren_divisa cod_ren_divisa_mis
		, RE.ds_per_nombre_apellido ds_ren_nombrecliente
		, IF(RE.cod_per_tipopersona = 'null', 'F', RE.cod_per_tipopersona) cod_per_tipopersona
		, RE.cod_ren_origen_inf ds_ren_intragrupo 
		, CONCAT(TRIM(RE.cod_ren_tipo_ajuste),TRIM(RE.cod_ren_origen_inf)) cod_ren_internegocio 
		, RE.cod_ren_producto_gest cod_productogest 
		, RE.cod_ren_segmento_gest cod_segmentogest 
		, RE.cod_ren_producto_generic cod_producto_operacional 
		, RE.dt_ren_reestruccontrato 
		, ROW_NUMBER() OVER(PARTITION BY SUBSTRING(RE.cod_ren_contrato, 1, 26)) int_ren_ordercontrato
		----------------------------
		, SUM(COALESCE(RE.fc_ren_resultado_total_ml, 0)) fc_ren_resultado_total_ml
		, SUM(COALESCE(RE.fc_ren_saldo_medio_ml, 0)) fc_ren_saldo_medio_ml
		, SUM(COALESCE(RE.fc_ren_sdo_med_cap_ml, 0)) fc_ren_sdo_med_cap_ml
		, SUM(COALESCE(RE.fc_ren_sdo_med_int_ml, 0)) fc_ren_sdo_med_int_ml
		, SUM(COALESCE(RE.fc_ren_sdo_med_reajuste_ml, 0)) fc_ren_sdo_med_reajuste_ml
		, SUM(COALESCE(RE.fc_ren_sdo_med_insolv_ml, 0)) fc_ren_sdo_med_insolv_ml
		, SUM(COALESCE(RE.fc_ren_ing_per_ml, 0)) fc_ren_ing_per_ml
		, SUM(COALESCE(RE.fc_ren_ing_cap_ml, 0)) fc_ren_ing_cap_ml
		, SUM(COALESCE(RE.fc_ren_resultado_total_real_ml, 0)) fc_ren_resultado_total_real_ml
		, RE.partition_date 
	FROM bi_corp_common.bt_ren_result RE , cto_contenido_a CA 
	WHERE RE.partition_date = '2020-10-31'
		AND RE.cod_ren_contrato = CA.cod_ren_contrato
		AND RE.cod_ren_contenido = CA.cod_ren_contenido
		AND RE.cod_ren_contrato_rorac = CA.cod_ren_contrato_rorac
		AND RE.cod_ren_contenido IN ('PRE','CRE','ARF','CCO','COM','CTB','ROF','CCL')
		AND RE.cod_ren_contrato != '-99100'
	GROUP BY RE.cod_ren_contrato
		, RE.cod_ren_cliente_rorac
		, RE.cod_ren_contrato_rorac
		, RE.cod_ren_pers_ods
		, RE.cod_ren_contenido
		, RE.cod_ren_area_negocio
		, RE.cod_ren_producto_niv_3
		, RE.dt_ren_altacontrato 
		, RE.dt_ren_vencontrato 
		, RE.cod_ren_entidad_espana 
		, RE.cod_ren_centro_cont 
		, IF(SUBSTRING(RE.cod_ren_contrato,11,4) = '', NULL, SUBSTRING(RE.cod_ren_contrato,11,4)) 
		, RE.cod_ren_gestor_prod 
		, IF(RE.cod_ren_gestor = 'null', NULL, RE.cod_ren_gestor)  
		, RE.cod_ren_cta_resultados_niv_9
		, RE.cod_ren_cta_resultados_niv_8
		, IF(RE.cod_ren_vincula = 'null', 0, RE.cod_ren_vincula) 
		, RE.cod_ren_divisa 
		, RE.ds_per_nombre_apellido 
		, IF(RE.cod_per_tipopersona = 'null', 'F', RE.cod_per_tipopersona) 
		, RE.cod_ren_origen_inf  
		, CONCAT(TRIM(RE.cod_ren_tipo_ajuste),TRIM(RE.cod_ren_origen_inf))  
		, RE.cod_ren_producto_gest  
		, RE.cod_ren_segmento_gest 
		, RE.cod_ren_producto_generic  
		, RE.dt_ren_reestruccontrato 
		, RE.partition_date
	UNION ALL 
	SELECT RE.cod_ren_contrato 
		, RE.cod_ren_cliente_rorac
		, RE.cod_ren_contrato_rorac
		, RE.cod_ren_pers_ods
		, RE.cod_ren_contenido
		, RE.cod_ren_area_negocio
		, RE.cod_ren_producto_niv_3
		, RE.dt_ren_altacontrato 
		, RE.dt_ren_vencontrato 
		, RE.cod_ren_entidad_espana 
		, RE.cod_ren_centro_cont cod_ren_centrocont
		, IF(SUBSTRING(RE.cod_ren_contrato,11,4) = '', NULL, SUBSTRING(RE.cod_ren_contrato,11,4)) cod_ren_subprodu
		, RE.cod_ren_gestor_prod 
		, IF(RE.cod_ren_gestor = 'null', NULL, RE.cod_ren_gestor) cod_ren_gestor 
		, RE.cod_ren_cta_resultados_niv_9
		, RE.cod_ren_cta_resultados_niv_8
		, IF(RE.cod_ren_vincula = 'null', 0, RE.cod_ren_vincula) cod_ren_vincula
		, RE.cod_ren_divisa cod_ren_divisa_mis
		, RE.ds_per_nombre_apellido ds_ren_nombrecliente
		, IF(RE.cod_per_tipopersona = 'null', 'F', RE.cod_per_tipopersona) cod_per_tipopersona
		, RE.cod_ren_origen_inf ds_ren_intragrupo 
		, CONCAT(TRIM(RE.cod_ren_tipo_ajuste),TRIM(RE.cod_ren_origen_inf)) cod_ren_internegocio 
		, RE.cod_ren_producto_gest cod_productogest 
		, RE.cod_ren_segmento_gest cod_segmentogest 
		, RE.cod_ren_producto_generic cod_producto_operacional 
		, RE.dt_ren_reestruccontrato 
		, NULL int_ren_ordercontrato
		----------------------------
		, SUM(COALESCE(RE.fc_ren_resultado_total_ml, 0)) fc_ren_resultado_total_ml
		, SUM(COALESCE(RE.fc_ren_saldo_medio_ml, 0)) fc_ren_saldo_medio_ml
		, SUM(COALESCE(RE.fc_ren_sdo_med_cap_ml, 0)) fc_ren_sdo_med_cap_ml
		, SUM(COALESCE(RE.fc_ren_sdo_med_int_ml, 0)) fc_ren_sdo_med_int_ml
		, SUM(COALESCE(RE.fc_ren_sdo_med_reajuste_ml, 0)) fc_ren_sdo_med_reajuste_ml
		, SUM(COALESCE(RE.fc_ren_sdo_med_insolv_ml, 0)) fc_ren_sdo_med_insolv_ml
		, SUM(COALESCE(RE.fc_ren_ing_per_ml, 0)) fc_ren_ing_per_ml
		, SUM(COALESCE(RE.fc_ren_ing_cap_ml, 0)) fc_ren_ing_cap_ml
		, SUM(COALESCE(RE.fc_ren_resultado_total_real_ml, 0)) fc_ren_resultado_total_real_ml
		, RE.partition_date 
	FROM bi_corp_common.bt_ren_result RE , cto_producto_a PA 
	WHERE RE.partition_date = '2020-10-31'
		AND RE.cod_ren_contrato = PA.cod_ren_contrato
		AND RE.cod_ren_contenido = PA.cod_ren_contenido
		AND RE.cod_ren_producto_gest = PA.cod_ren_producto_gest
		AND RE.cod_ren_contenido IN ('COM','CTB','ROF','CCL')
		AND RE.cod_ren_contrato = '-99100'
	GROUP BY RE.cod_ren_contrato
		, RE.cod_ren_cliente_rorac
		, RE.cod_ren_contrato_rorac
		, RE.cod_ren_pers_ods
		, RE.cod_ren_contenido
		, RE.cod_ren_area_negocio
		, RE.cod_ren_producto_niv_3
		, RE.dt_ren_altacontrato 
		, RE.dt_ren_vencontrato 
		, RE.cod_ren_entidad_espana 
		, RE.cod_ren_centro_cont 
		, IF(SUBSTRING(RE.cod_ren_contrato,11,4) = '', NULL, SUBSTRING(RE.cod_ren_contrato,11,4)) 
		, RE.cod_ren_gestor_prod 
		, IF(RE.cod_ren_gestor = 'null', NULL, RE.cod_ren_gestor)  
		, RE.cod_ren_cta_resultados_niv_9
		, RE.cod_ren_cta_resultados_niv_8
		, IF(RE.cod_ren_vincula = 'null', 0, RE.cod_ren_vincula) 
		, RE.cod_ren_divisa 
		, RE.ds_per_nombre_apellido 
		, IF(RE.cod_per_tipopersona = 'null', 'F', RE.cod_per_tipopersona) 
		, RE.cod_ren_origen_inf  
		, CONCAT(TRIM(RE.cod_ren_tipo_ajuste),TRIM(RE.cod_ren_origen_inf))  
		, RE.cod_ren_producto_gest  
		, RE.cod_ren_segmento_gest 
		, RE.cod_ren_producto_generic  
		, RE.dt_ren_reestruccontrato 
		, RE.partition_date	;

------------------------------------------------
set hive.execution.engine=spark;
set spark.yarn.queue=root.dataeng;
----------------------------------------- PASIVO
CREATE TEMPORARY TABLE cto_pasivo AS
	SELECT RE.cod_ren_contrato
		, RE.cod_ren_cliente_rorac
		, RE.cod_ren_contrato_rorac
		, RE.cod_ren_pers_ods
		, RE.cod_ren_contenido
		, RE.cod_ren_area_negocio
		, RE.cod_ren_producto_niv_3
		, RE.dt_ren_altacontrato 
		, RE.dt_ren_vencontrato 
		, RE.cod_ren_entidad_espana 
		, RE.cod_ren_centro_cont cod_ren_centrocont
		, IF(SUBSTRING(RE.cod_ren_contrato,11,4) = '', NULL, SUBSTRING(RE.cod_ren_contrato,11,4)) cod_ren_subprodu
		, RE.cod_ren_gestor_prod 
		, IF(RE.cod_ren_gestor = 'null', NULL, RE.cod_ren_gestor) cod_ren_gestor 
		, RE.cod_ren_cta_resultados_niv_9
		, RE.cod_ren_cta_resultados_niv_8
		, IF(RE.cod_ren_vincula = 'null', 0, RE.cod_ren_vincula) cod_ren_vincula
		, RE.cod_ren_divisa cod_ren_divisa_mis
		, RE.ds_per_nombre_apellido ds_ren_nombrecliente
		, IF(RE.cod_per_tipopersona = 'null', 'F', RE.cod_per_tipopersona) cod_per_tipopersona
		, RE.cod_ren_origen_inf ds_ren_intragrupo 
		, CONCAT(TRIM(RE.cod_ren_tipo_ajuste),TRIM(RE.cod_ren_origen_inf)) cod_ren_internegocio 
		, RE.cod_ren_producto_gest cod_productogest 
		, RE.cod_ren_segmento_gest cod_segmentogest 
		, RE.cod_ren_producto_generic cod_producto_operacional 
		, RE.dt_ren_reestruccontrato 
		, ROW_NUMBER() OVER(PARTITION BY SUBSTRING(RE.cod_ren_contrato, 1, 26)) int_ren_ordercontrato
		----------------------------
		, SUM(COALESCE(RE.fc_ren_resultado_total_ml, 0)) fc_ren_resultado_total_ml
		, SUM(COALESCE(RE.fc_ren_saldo_medio_ml, 0)) fc_ren_saldo_medio_ml
		, SUM(COALESCE(RE.fc_ren_sdo_med_cap_ml, 0)) fc_ren_sdo_med_cap_ml
		, SUM(COALESCE(RE.fc_ren_sdo_med_int_ml, 0)) fc_ren_sdo_med_int_ml
		, SUM(COALESCE(RE.fc_ren_sdo_med_reajuste_ml, 0)) fc_ren_sdo_med_reajuste_ml
		, SUM(COALESCE(RE.fc_ren_sdo_med_insolv_ml, 0)) fc_ren_sdo_med_insolv_ml
		, SUM(COALESCE(RE.fc_ren_ing_per_ml, 0)) fc_ren_ing_per_ml
		, SUM(COALESCE(RE.fc_ren_ing_cap_ml, 0)) fc_ren_ing_cap_ml
		, SUM(COALESCE(RE.fc_ren_resultado_total_real_ml, 0)) fc_ren_resultado_total_real_ml
		, RE.partition_date 
	FROM bi_corp_common.bt_ren_result RE , cto_contenido_p CP 
	WHERE RE.partition_date = '2020-10-31'
		AND RE.cod_ren_contrato = CP.cod_ren_contrato
		AND RE.cod_ren_contenido = CP.cod_ren_contenido
		AND RE.cod_ren_contrato_rorac = CP.cod_ren_contrato_rorac
		AND RE.cod_ren_contenido IN ('PLZ','CTA','COM','CTB','ROF','CCL')
		AND RE.cod_ren_contrato != '-99100'
	GROUP BY RE.cod_ren_contrato
		, RE.cod_ren_cliente_rorac
		, RE.cod_ren_contrato_rorac
		, RE.cod_ren_pers_ods
		, RE.cod_ren_contenido
		, RE.cod_ren_area_negocio
		, RE.cod_ren_producto_niv_3
		, RE.dt_ren_altacontrato 
		, RE.dt_ren_vencontrato 
		, RE.cod_ren_entidad_espana 
		, RE.cod_ren_centro_cont 
		, IF(SUBSTRING(RE.cod_ren_contrato,11,4) = '', NULL, SUBSTRING(RE.cod_ren_contrato,11,4)) 
		, RE.cod_ren_gestor_prod 
		, IF(RE.cod_ren_gestor = 'null', NULL, RE.cod_ren_gestor) 
		, RE.cod_ren_cta_resultados_niv_9
		, RE.cod_ren_cta_resultados_niv_8
		, IF(RE.cod_ren_vincula = 'null', 0, RE.cod_ren_vincula) 
		, RE.cod_ren_divisa 
		, RE.ds_per_nombre_apellido 
		, IF(RE.cod_per_tipopersona = 'null', 'F', RE.cod_per_tipopersona) 
		, RE.cod_ren_origen_inf  
		, CONCAT(TRIM(RE.cod_ren_tipo_ajuste),TRIM(RE.cod_ren_origen_inf))  
		, RE.cod_ren_producto_gest 
		, RE.cod_ren_segmento_gest  
		, RE.cod_ren_producto_generic 
		, RE.dt_ren_reestruccontrato 
		, RE.partition_date
	UNION ALL 
	SELECT RE.cod_ren_contrato 
		, RE.cod_ren_cliente_rorac
		, RE.cod_ren_contrato_rorac
		, RE.cod_ren_pers_ods
		, RE.cod_ren_contenido
		, RE.cod_ren_area_negocio
		, RE.cod_ren_producto_niv_3
		, RE.dt_ren_altacontrato 
		, RE.dt_ren_vencontrato 
		, RE.cod_ren_entidad_espana 
		, RE.cod_ren_centro_cont cod_ren_centrocont
		, IF(SUBSTRING(RE.cod_ren_contrato,11,4) = '', NULL, SUBSTRING(RE.cod_ren_contrato,11,4)) cod_ren_subprodu
		, RE.cod_ren_gestor_prod 
		, IF(RE.cod_ren_gestor = 'null', NULL, RE.cod_ren_gestor) cod_ren_gestor 
		, RE.cod_ren_cta_resultados_niv_9
		, RE.cod_ren_cta_resultados_niv_8
		, IF(RE.cod_ren_vincula = 'null', 0, RE.cod_ren_vincula) cod_ren_vincula
		, RE.cod_ren_divisa cod_ren_divisa_mis
		, RE.ds_per_nombre_apellido ds_ren_nombrecliente
		, IF(RE.cod_per_tipopersona = 'null', 'F', RE.cod_per_tipopersona) cod_per_tipopersona
		, RE.cod_ren_origen_inf ds_ren_intragrupo 
		, CONCAT(TRIM(RE.cod_ren_tipo_ajuste),TRIM(RE.cod_ren_origen_inf)) cod_ren_internegocio 
		, RE.cod_ren_producto_gest cod_productogest 
		, RE.cod_ren_segmento_gest cod_segmentogest 
		, RE.cod_ren_producto_generic cod_producto_operacional 
		, RE.dt_ren_reestruccontrato 
		, NULL int_ren_ordercontrato
		----------------------------
		, SUM(COALESCE(RE.fc_ren_resultado_total_ml, 0)) fc_ren_resultado_total_ml
		, SUM(COALESCE(RE.fc_ren_saldo_medio_ml, 0)) fc_ren_saldo_medio_ml
		, SUM(COALESCE(RE.fc_ren_sdo_med_cap_ml, 0)) fc_ren_sdo_med_cap_ml
		, SUM(COALESCE(RE.fc_ren_sdo_med_int_ml, 0)) fc_ren_sdo_med_int_ml
		, SUM(COALESCE(RE.fc_ren_sdo_med_reajuste_ml, 0)) fc_ren_sdo_med_reajuste_ml
		, SUM(COALESCE(RE.fc_ren_sdo_med_insolv_ml, 0)) fc_ren_sdo_med_insolv_ml
		, SUM(COALESCE(RE.fc_ren_ing_per_ml, 0)) fc_ren_ing_per_ml
		, SUM(COALESCE(RE.fc_ren_ing_cap_ml, 0)) fc_ren_ing_cap_ml
		, SUM(COALESCE(RE.fc_ren_resultado_total_real_ml, 0)) fc_ren_resultado_total_real_ml
		, RE.partition_date 
	FROM bi_corp_common.bt_ren_result RE , cto_producto_p PP 
	WHERE RE.partition_date = '2020-10-31'
		AND RE.cod_ren_contrato = PP.cod_ren_contrato
		AND RE.cod_ren_contenido = PP.cod_ren_contenido
		AND RE.cod_ren_producto_gest = PP.cod_ren_producto_gest
		AND RE.cod_ren_contenido IN ('COM','CTB','ROF','CCL')
		AND RE.cod_ren_contrato = '-99100'
	GROUP BY RE.cod_ren_contrato
		, RE.cod_ren_cliente_rorac
		, RE.cod_ren_contrato_rorac
		, RE.cod_ren_pers_ods
		, RE.cod_ren_contenido
		, RE.cod_ren_area_negocio
		, RE.cod_ren_producto_niv_3
		, RE.dt_ren_altacontrato 
		, RE.dt_ren_vencontrato 
		, RE.cod_ren_entidad_espana 
		, RE.cod_ren_centro_cont 
		, IF(SUBSTRING(RE.cod_ren_contrato,11,4) = '', NULL, SUBSTRING(RE.cod_ren_contrato,11,4)) 
		, RE.cod_ren_gestor_prod 
		, IF(RE.cod_ren_gestor = 'null', NULL, RE.cod_ren_gestor)  
		, RE.cod_ren_cta_resultados_niv_9
		, RE.cod_ren_cta_resultados_niv_8
		, IF(RE.cod_ren_vincula = 'null', 0, RE.cod_ren_vincula) 
		, RE.cod_ren_divisa 
		, RE.ds_per_nombre_apellido 
		, IF(RE.cod_per_tipopersona = 'null', 'F', RE.cod_per_tipopersona) 
		, RE.cod_ren_origen_inf  
		, CONCAT(TRIM(RE.cod_ren_tipo_ajuste),TRIM(RE.cod_ren_origen_inf))  
		, RE.cod_ren_producto_gest  
		, RE.cod_ren_segmento_gest 
		, RE.cod_ren_producto_generic  
		, RE.dt_ren_reestruccontrato 
		, RE.partition_date	;

-------------------------------------------------------------------------------
------------------------------------------------------------- TEMP_PRODUCTO_MIS
set hive.execution.engine=spark;
set spark.yarn.queue=root.dataeng;
-----------------------------------------
CREATE TEMPORARY TABLE IF NOT EXISTS producto_mis AS
	SELECT TRIM(cod_valor) cod_producto_gest
		, TRIM(cod_valor_padre) cod_producto_mis -- 20201013
		, TRIM(num_nivel) num_nivel
	FROM bi_corp_staging.rio157_ms0_dm_dwh_composicion_jerqs 
	WHERE partition_date = '2020-10-09'
		AND cod_jerarquia = 'JBP01'
		AND cod_dimension = 'PR01' ;

-------------------------------------------------------------------------------
------------------------------------------------------------- TEMP_TABLON_IFRS9
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
	WHERE RI.periodo = '20201031'
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

----------------------------------------------------------------------------------------------
----------------------------------------------------------------------- -TEMP_ROSETTA_KEY
CREATE TEMPORARY TABLE IF NOT EXISTS rosetta_key AS 
	SELECT MK.native_key AS cod_ren_contrato_rorac
		 , BDR.native_key AS id_cto_bdr
	FROM bi_corp_common.rosetta_nkey_hist MK
		JOIN bi_corp_common.rosetta_nkey_hist BDR ON 
			MK.master_key = BDR.master_key 
			AND BDR.partition_date = '2020-10-31'
			AND BDR.domain_code = '00004'
	WHERE MK.partition_date = '2020-10-31'
		AND MK.domain_code = '00005';

----------------------------------------------------------------------------------------------
----------------------------------------------------------------------- INSERT ACTIVO
set mapred.job.queue.name=root.dataeng;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=40000000;
-----------------------------------------------------------
INSERT OVERWRITE TABLE bi_corp_common.bt_ror_input_activo
PARTITION(partition_date) 
-----------------------------------------------------------
	SELECT 'ARG' cod_ren_unidad ,
		date_format(RE.partition_date, 'yyyyMM') dt_ren_data ,
		1 cod_ren_finalidaddatos ,
		IF(RE.cod_ren_contenido IN ('CCL','CTB'), RE.cod_ren_cliente_rorac, RE.cod_ren_contrato_rorac) cod_ren_contrato_rorac ,
		RE.cod_ren_contrato ,
		RE.cod_ren_area_negocio cod_ren_areanegocio ,
		'ARS' cod_ren_divisa ,
		adn2.cod_ren_area_negocio_niv_2 cod_ren_division ,
		pm.cod_producto_mis cod_ren_producto ,
		RE.cod_ren_pers_ods ,
		CASE WHEN RE.cod_ren_producto_niv_3 = 'BG1'THEN SUM(RE.fc_ren_resultado_total_ml) ELSE 0 END fc_ren_margenmes ,
		CASE WHEN RE.cod_ren_producto_niv_3 = 'BG1'THEN SUM(RE.fc_ren_saldo_medio_ml) ELSE 0 END fc_ren_sdbsmv ,
		CASE WHEN RE.cod_ren_producto_niv_3 = 'BG3' THEN (SUM(RE.fc_ren_sdo_med_cap_ml)
											+ SUM(RE.fc_ren_sdo_med_int_ml)
											+ SUM(RE.fc_ren_sdo_med_reajuste_ml)
											+ SUM(RE.fc_ren_sdo_med_insolv_ml)) ELSE 0 END fc_ren_sfbsmv ,
		RE.dt_ren_altacontrato ,
		RE.dt_ren_vencontrato ,
		RE.cod_ren_entidad_espana ,
		RE.cod_ren_centrocont ,
		RE.cod_ren_subprodu ,
		NULL cod_ren_zona ,
		NULL cod_ren_territorial ,
		RE.cod_ren_gestor_prod ,
		RE.cod_ren_gestor ,
		RK.id_cto_bdr id_cto_bdr ,
		CASE WHEN RE.cod_ren_producto_niv_3 = 'BG1' THEN (SUM(RE.fc_ren_ing_per_ml)
											+ SUM(RE.fc_ren_ing_cap_ml)
											+ SUM(RE.fc_ren_sdo_med_cap_ml)
											+ SUM(RE.fc_ren_sdo_med_int_ml)
											+ SUM(RE.fc_ren_sdo_med_reajuste_ml)
											+ SUM(RE.fc_ren_sdo_med_insolv_ml)) ELSE 0 END fc_ren_sdbsfm ,
		CASE WHEN RE.cod_ren_producto_niv_3 = 'BG1' AND RE.cod_ren_cta_resultados_niv_9 = '1031' THEN SUM(RE.fc_ren_resultado_total_real_ml) ELSE 0 END fc_ren_interes ,
		CASE WHEN RE.cod_ren_producto_niv_3 = 'BG1' AND RE.cod_ren_cta_resultados_niv_9 = '1032' THEN SUM(RE.fc_ren_resultado_total_real_ml) ELSE 0 END fc_ren_comfin ,
		CASE WHEN RE.cod_ren_producto_niv_3 = 'BG1' AND RE.cod_ren_cta_resultados_niv_8 = '1051' THEN SUM(RE.fc_ren_resultado_total_real_ml) ELSE 0 END fc_ren_comnofin ,
		RE.cod_ren_vincula ,
		CASE WHEN RE.cod_ren_producto_niv_3 = 'BG1' AND RE.cod_ren_cta_resultados_niv_8 = '1052' THEN SUM(RE.fc_ren_resultado_total_real_ml) ELSE 0 END fc_ren_rof ,
		SUM(RE.fc_ren_resultado_total_ml) fc_ren_tti ,
		NULL cod_ren_mtm ,
		NULL cod_ren_nocional ,
		RE.cod_ren_divisa_mis ,
		NULL flag_ren_moralocal ,
		NULL flag_ren_carterizadas ,
		RE.ds_ren_nombrecliente ,
		RE.cod_per_tipopersona ,
		NULL cod_ren_nifcif ,
		RE.ds_ren_intragrupo ,
		RE.cod_ren_internegocio ,
		adn.cod_ren_primercorporativo  cod_ren_areanegociocorp ,
		RE.cod_productogest ,
		RE.cod_segmentogest ,
		RE.cod_producto_operacional ,
		CONCAT(regexp_replace(RE.partition_date, '-', ''), 'v1') ds_ren_ficheromis ,
		RE.dt_ren_reestruccontrato ,
		date_format(RE.partition_date, 'dd-MM-yyyy') dt_ren_fec_extrdatos ,
		IF(RE.cod_ren_contenido = 'CCL', 2, 0) flag_ren_neteo ,
		CASE WHEN RE.cod_ren_producto_niv_3 = 'BG1' AND RE.cod_ren_cta_resultados_niv_8 = '1054' THEN SUM(RE.fc_ren_resultado_total_real_ml) ELSE 0 END fc_ren_orex ,
		SUM(COALESCE(RI.s_provision_amount, 0)) fc_ifrs_provision ,
		NULL fc_ren_costemensualcto ,
	    IF(RE.cod_ren_contenido NOT IN ('CCL','CTB'), 0, IF(RE.cod_ren_pers_ods != '-99100', 1, 3)) cod_nivel_operacion ,
	    RE.partition_date
	FROM cto_activo RE
		-------------------------------------------------------AREANEGOCIOCTR
		LEFT JOIN bi_corp_common.dim_ren_areanegocioctr adn2 ON 
			RE.cod_ren_area_negocio = adn2.cod_ren_area_negocio_niv_9 
		-------------------------------------------------------PRODUCTOMIS
		LEFT JOIN producto_mis pm ON 
			RE.cod_productogest = PM.cod_producto_gest
		-------------------------------------------------------AREANEGOCIOCORP
		LEFT JOIN bi_corp_common.dim_ren_jeareanegocioctr adn ON 
			RE.cod_ren_area_negocio = adn.cod_ren_areanegociohijo
		-------------------------------------------------------ROSETTA
		LEFT JOIN rosetta_key RK ON 
			RE.cod_ren_contrato_rorac = RK.cod_ren_contrato_rorac
		-------------------------------------------------------IFRS
		LEFT JOIN tablon_ifrs9 RI ON 
			SUBSTRING(RE.cod_ren_contrato,1,26) = RI.cod_ren_contrato_short
			AND RE.int_ren_ordercontrato = 1 
		-------------------------------------------------------
	GROUP BY date_format(RE.partition_date, 'yyyyMM') ,
			IF(RE.cod_ren_contenido IN ('CCL','CTB'), RE.cod_ren_cliente_rorac, RE.cod_ren_contrato_rorac) ,
			RE.cod_ren_contrato ,
			RE.cod_ren_area_negocio ,
			adn2.cod_ren_area_negocio_niv_2 ,
			pm.cod_producto_mis ,
			RE.cod_ren_pers_ods ,
			RE.dt_ren_altacontrato ,
			RE.dt_ren_vencontrato ,
			RE.cod_ren_entidad_espana ,
			RE.cod_ren_centrocont ,
			RE.cod_ren_subprodu ,
			RE.cod_ren_gestor_prod ,
			RE.cod_ren_gestor ,
			RK.id_cto_bdr ,
			RE.cod_ren_vincula ,
			RE.cod_ren_divisa_mis ,
			RE.ds_ren_nombrecliente ,
			RE.cod_per_tipopersona ,
			RE.ds_ren_intragrupo ,
			RE.cod_ren_internegocio ,
			adn.cod_ren_primercorporativo ,
			RE.cod_productogest ,
			RE.cod_segmentogest ,
			RE.cod_producto_operacional ,
			CONCAT(regexp_replace(RE.partition_date, '-', ''), 'v1') ,
			RE.dt_ren_reestruccontrato ,
			date_format(RE.partition_date, 'dd-MM-yyyy') ,
			IF(RE.cod_ren_contenido = 'CCL', 2, 0) ,
			IF(RE.cod_ren_contenido NOT IN ('CCL','CTB'), 0, IF(RE.cod_ren_pers_ods != '-99100', 1, 3)) ,
		    RE.partition_date ,
		    RE.cod_ren_producto_niv_3 ,
		    RE.cod_ren_cta_resultados_niv_8 ,
		    RE.cod_ren_cta_resultados_niv_9 ;


-------------------------------------------------------------------------------------
----------------------------------------------------------------------- INSERT PASIVO
set mapred.job.queue.name=root.dataeng;
set hive.merge.mapfiles=true;
set hive.merge.mapredfiles=true;
set hive.merge.size.per.task=4000000;
set hive.merge.smallfiles.avgsize=16000000;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.auto.convert.join=true;
set hive.auto.convert.join.noconditionaltask=true;
set hive.auto.convert.join.noconditionaltask.size=40000000;
-----------------------------------------------------------
INSERT OVERWRITE TABLE bi_corp_common.bt_ror_input_pasivo
PARTITION(partition_date) 
-----------------------------------------------------------
	SELECT 'ARG' cod_ren_unidad ,
		date_format(RE.partition_date, 'yyyyMM') dt_ren_data ,
		1 cod_ren_finalidaddatos ,
		IF(RE.cod_ren_contenido IN ('CCL','CTB'), RE.cod_ren_cliente_rorac, RE.cod_ren_contrato_rorac) cod_ren_contrato_rorac ,
		RE.cod_ren_contrato ,
		RE.cod_ren_area_negocio cod_ren_areanegocio ,
		'ARS' cod_ren_divisa ,
		adn2.cod_ren_area_negocio_niv_2 cod_ren_division ,
		pm.cod_producto_mis cod_ren_producto ,
		RE.cod_ren_pers_ods ,
		CASE WHEN RE.cod_ren_producto_niv_3 = 'BG1'THEN SUM(RE.fc_ren_resultado_total_ml) ELSE 0 END fc_ren_margenmes ,
		CASE WHEN RE.cod_ren_producto_niv_3 = 'BG1'THEN SUM(RE.fc_ren_saldo_medio_ml) ELSE 0 END fc_ren_sdbsmv ,
		CASE WHEN RE.cod_ren_producto_niv_3 = 'BG3' THEN (SUM(RE.fc_ren_sdo_med_cap_ml)
											+ SUM(RE.fc_ren_sdo_med_int_ml)
											+ SUM(RE.fc_ren_sdo_med_reajuste_ml)
											+ SUM(RE.fc_ren_sdo_med_insolv_ml)) ELSE 0 END fc_ren_sfbsmv ,
		RE.dt_ren_altacontrato ,
		RE.dt_ren_vencontrato ,
		RE.cod_ren_entidad_espana ,
		RE.cod_ren_centrocont ,
		RE.cod_ren_subprodu ,
		NULL cod_ren_zona ,
		NULL cod_ren_territorial ,
		RE.cod_ren_gestor_prod ,
		RE.cod_ren_gestor ,
		RK.id_cto_bdr id_cto_bdr ,
		CASE WHEN RE.cod_ren_producto_niv_3 = 'BG1' THEN (SUM(RE.fc_ren_ing_per_ml)
											+ SUM(RE.fc_ren_ing_cap_ml)
											+ SUM(RE.fc_ren_sdo_med_cap_ml)
											+ SUM(RE.fc_ren_sdo_med_int_ml)
											+ SUM(RE.fc_ren_sdo_med_reajuste_ml)
											+ SUM(RE.fc_ren_sdo_med_insolv_ml)) ELSE 0 END fc_ren_sdbsfm ,
		CASE WHEN RE.cod_ren_producto_niv_3 = 'BG1' AND RE.cod_ren_cta_resultados_niv_9 = '1031' THEN SUM(RE.fc_ren_resultado_total_real_ml) ELSE 0 END fc_ren_interes ,
		CASE WHEN RE.cod_ren_producto_niv_3 = 'BG1' AND RE.cod_ren_cta_resultados_niv_9 = '1032' THEN SUM(RE.fc_ren_resultado_total_real_ml) ELSE 0 END fc_ren_comfin ,
		CASE WHEN RE.cod_ren_producto_niv_3 = 'BG1' AND RE.cod_ren_cta_resultados_niv_8 = '1051' THEN SUM(RE.fc_ren_resultado_total_real_ml) ELSE 0 END fc_ren_comnofin ,
		RE.cod_ren_vincula ,
		CASE WHEN RE.cod_ren_producto_niv_3 = 'BG1' AND RE.cod_ren_cta_resultados_niv_8 = '1052' THEN SUM(RE.fc_ren_resultado_total_real_ml) ELSE 0 END fc_ren_rof ,
		SUM(RE.fc_ren_resultado_total_ml) fc_ren_tti ,
		NULL cod_ren_mtm ,
		NULL cod_ren_nocional ,
		RE.cod_ren_divisa_mis ,
		NULL flag_ren_moralocal ,
		NULL flag_ren_carterizadas ,
		RE.ds_ren_nombrecliente ,
		RE.cod_per_tipopersona ,
		NULL cod_ren_nifcif ,
		RE.ds_ren_intragrupo ,
		RE.cod_ren_internegocio ,
		adn.cod_ren_primercorporativo  cod_ren_areanegociocorp ,
		RE.cod_productogest ,
		RE.cod_segmentogest ,
		RE.cod_producto_operacional ,
		CONCAT(regexp_replace(RE.partition_date, '-', ''), 'v1') ds_ren_ficheromis ,
		RE.dt_ren_reestruccontrato ,
		date_format(RE.partition_date, 'dd-MM-yyyy') dt_ren_fec_extrdatos ,
		IF(RE.cod_ren_contenido = 'CCL', 2, 0) flag_ren_neteo ,
		CASE WHEN RE.cod_ren_producto_niv_3 = 'BG1' AND RE.cod_ren_cta_resultados_niv_8 = '1054' THEN SUM(RE.fc_ren_resultado_total_real_ml) ELSE 0 END fc_ren_orex ,
		SUM(COALESCE(RI.s_provision_amount, 0)) fc_ifrs_provision ,
		NULL fc_ren_costemensualcto ,
	    IF(RE.cod_ren_contenido NOT IN ('CCL','CTB'), 0, IF(RE.cod_ren_pers_ods != '-99100', 1, 3)) cod_nivel_operacion ,
	    RE.partition_date
	FROM cto_pasivo RE
		-------------------------------------------------------AREANEGOCIOCTR
		LEFT JOIN bi_corp_common.dim_ren_areanegocioctr adn2 ON 
			RE.cod_ren_area_negocio = adn2.cod_ren_area_negocio_niv_9 
		-------------------------------------------------------PRODUCTOMIS
		LEFT JOIN producto_mis pm ON 
			RE.cod_productogest = PM.cod_producto_gest
		-------------------------------------------------------AREANEGOCIOCORP
		LEFT JOIN bi_corp_common.dim_ren_jeareanegocioctr adn ON 
			RE.cod_ren_area_negocio = adn.cod_ren_areanegociohijo
		-------------------------------------------------------ROSETTA
		LEFT JOIN rosetta_key RK ON 
			RE.cod_ren_contrato_rorac = RK.cod_ren_contrato_rorac
		-------------------------------------------------------IFRS
		LEFT JOIN tablon_ifrs9 RI ON 
			SUBSTRING(RE.cod_ren_contrato,1,26) = RI.cod_ren_contrato_short
			AND RE.int_ren_ordercontrato = 1 
		-------------------------------------------------------
	GROUP BY date_format(RE.partition_date, 'yyyyMM') ,
			IF(RE.cod_ren_contenido IN ('CCL','CTB'), RE.cod_ren_cliente_rorac, RE.cod_ren_contrato_rorac) ,
			RE.cod_ren_contrato ,
			RE.cod_ren_area_negocio ,
			adn2.cod_ren_area_negocio_niv_2 ,
			pm.cod_producto_mis ,
			RE.cod_ren_pers_ods ,
			RE.dt_ren_altacontrato ,
			RE.dt_ren_vencontrato ,
			RE.cod_ren_entidad_espana ,
			RE.cod_ren_centrocont ,
			RE.cod_ren_subprodu ,
			RE.cod_ren_gestor_prod ,
			RE.cod_ren_gestor ,
			RK.id_cto_bdr ,
			RE.cod_ren_vincula ,
			RE.cod_ren_divisa_mis ,
			RE.ds_ren_nombrecliente ,
			RE.cod_per_tipopersona ,
			RE.ds_ren_intragrupo ,
			RE.cod_ren_internegocio ,
			adn.cod_ren_primercorporativo ,
			RE.cod_productogest ,
			RE.cod_segmentogest ,
			RE.cod_producto_operacional ,
			CONCAT(regexp_replace(RE.partition_date, '-', ''), 'v1') ,
			RE.dt_ren_reestruccontrato ,
			date_format(RE.partition_date, 'dd-MM-yyyy') ,
			IF(RE.cod_ren_contenido = 'CCL', 2, 0) ,
			IF(RE.cod_ren_contenido NOT IN ('CCL','CTB'), 0, IF(RE.cod_ren_pers_ods != '-99100', 1, 3)) ,
		    RE.partition_date ,
		    RE.cod_ren_producto_niv_3 ,
		    RE.cod_ren_cta_resultados_niv_8 ,
		    RE.cod_ren_cta_resultados_niv_9 ;
	   
---------------------------------------------------------------------------------
----------------------------------------------------------------------- EXPORT	   
	   
SELECT
	RE.cod_ren_unidad unidad,
	date_format(RE.partition_date , 'yyyyMM') fechadatos,
	RE.cod_ren_finalidaddatos finalidaddatos,
	RE.cod_ren_contrato_rorac idcontratomis,
	RE.cod_ren_areanegocio idareadenegociolocal,
	RE.cod_ren_divisa divisa,
	RE.cod_ren_division iddivision,
	RE.cod_ren_producto idproductomis,
	'' idru,
	RE.cod_ren_pers_ods idclientemis,
	23100 idempresabdr,
	LPAD(CAST(SUBSTRING(RE.cod_ren_pers_ods, 6, 8) AS INT),9,'0') idclientebdr,
	SUM(RE.fc_ren_margenmes) margenmes,
	SUM(RE.fc_ren_sdbsmv) sdbsmv,
	SUM(RE.fc_ren_sfbsmv) sfbsmv,
	'' eadmmff,
	0 flagmmff,
	IF(RE.dt_ren_fec_altacto IS NULL, date_format(RE.partition_date , 'dd-MM-yyyy'), date_format(RE.dt_ren_fec_altacto, 'dd-MM-yyyy')) fecapertura ,
	IF(RE.dt_ren_fec_ven IS NULL,'01-01-9999',date_format(RE.dt_ren_fec_ven, 'dd-MM-yyyy')) fecvcto,
	RE.cod_ren_entidad_espana identidadmis,
	RE.cod_ren_centrocont idcentro,
	RE.cod_ren_subprodu idsubproductooperacional,
	'' zona,
	'' territorial,
	IF(RE.cod_ren_gestorprod = 'N/A',NULL,RE.cod_ren_gestorprod) gestorproducto,
	IF(RE.cod_ren_gestor = 'null',NULL,RE.cod_ren_gestor) gestorcliente,
	RE.id_cto_bdr,
	SUM(RE.fc_ren_sdbsfm) sdbsfm,
	'' sfbsfm,
	SUM(RE.fc_ren_interes) intereses,
	SUM(RE.fc_ren_comfin) comfin,
	SUM(RE.fc_ren_comnofin) comnofin,
	IF(RE.cod_ren_vincula = 'null',0,RE.cod_ren_vincula) codvincula,
	SUM(RE.fc_ren_rof) rof,
	MAX(RE.fc_ren_tti) tti,
	'' mtm,
	'' nocional,
	RE.cod_ren_divisa_mis coddivmis,
	RE.flag_ren_moralocal flagmoralocal,
	RE.flag_ren_carterizadas flagcarterizadas,
	RE.ds_ren_nombrecliente nombreclien,
	COALESCE(RE.cod_per_tipopersona,'F') tippers,
	'' nifcif,
	IF(RE.ds_ren_intragrupo = 'OPERACIONAL',1,0) intragrupo,
	RE.cod_ren_internegocio internegocio,
	RE.cod_ren_areanegociocorp areadenegociocorp,
	RE.cod_productogest idproductogestion,
	RE.cod_segmentogest segmentoclientemis,
	RE.cod_producto_operacional idproductooperacional,
	RE.ds_ren_ficheromis ficheromis,
	date_format(RE.dt_ren_fec_reestruc, 'dd-MM-yyyy') fecrenov,
	RE.dt_ren_fec_extrdatos fecextrdatosmis,
	RE.flag_ren_neteo flagneteo,
	SUM(RE.fc_ifrs_provision) provision,
	'' stagedeprovision,
	SUM(RE.fc_ren_orex) orex,
	'' costesmis,
	RE.cod_nivel_operacion niveloperacion,
	'' nombregestorproducto,
	'' nombregestorcliente,
	'' dotacion
FROM
	bi_corp_common.bt_ror_input_activo RE
WHERE
	partition_date = '$month_to'
GROUP BY
	RE.cod_ren_unidad,
	RE.partition_date ,
	RE.cod_ren_finalidaddatos,
	RE.cod_ren_contrato_rorac,
	RE.cod_ren_areanegocio,
	RE.cod_ren_divisa,
	RE.cod_ren_division,
	RE.cod_ren_producto,
	RE.cod_ren_pers_ods,
	LPAD(CAST(SUBSTRING(RE.cod_ren_pers_ods, 6, 8) AS INT),9,'0'),
	IF(RE.dt_ren_fec_altacto IS NULL, date_format(RE.partition_date , 'dd-MM-yyyy'), date_format(RE.dt_ren_fec_altacto, 'dd-MM-yyyy')),
	IF(RE.dt_ren_fec_ven IS NULL,'01-01-9999',date_format(RE.dt_ren_fec_ven, 'dd-MM-yyyy')),
	RE.cod_ren_entidad_espana,
	RE.cod_ren_centrocont,
	RE.cod_ren_subprodu,
	IF(RE.cod_ren_gestorprod = 'N/A',NULL,RE.cod_ren_gestorprod),
	IF(RE.cod_ren_gestor = 'null',NULL,RE.cod_ren_gestor),
	RE.id_cto_bdr,
	IF(RE.cod_ren_vincula = 'null',0,RE.cod_ren_vincula),
	RE.cod_ren_divisa_mis,
	RE.flag_ren_moralocal ,
	RE.flag_ren_carterizadas ,
	RE.ds_ren_nombrecliente ,
	COALESCE(RE.cod_per_tipopersona,'F') ,
	IF(RE.ds_ren_intragrupo = 'OPERACIONAL',1,0) ,
	RE.cod_ren_internegocio ,
	RE.cod_ren_areanegociocorp ,
	RE.cod_productogest ,
	RE.cod_segmentogest ,
	RE.cod_producto_operacional ,
	RE.ds_ren_ficheromis ,
	date_format(RE.dt_ren_fec_reestruc, 'dd-MM-yyyy') ,
	RE.dt_ren_fec_extrdatos ,
	RE.flag_ren_neteo ,
	RE.cod_nivel_operacion ;



SELECT
	RE.cod_ren_unidad unidad,
	date_format(RE.partition_date , 'yyyyMM') fechadatos,
	RE.cod_ren_finalidaddatos finalidaddatos,
	RE.cod_ren_contrato_rorac idcontratomis,
	RE.cod_ren_areanegocio idareadenegociolocal,
	RE.cod_ren_divisa divisa,
	RE.cod_ren_division iddivision,
	RE.cod_ren_producto idproductomis,
	RE.cod_ren_pers_ods idclientemis,
	SUM(RE.fc_ren_margenmes) margenmes,
	SUM(RE.fc_ren_sdbsmv) sdbsmv,
	SUM(RE.fc_ren_sfbsmv) sfbsmv,
	IF(RE.dt_ren_fec_altacto IS NULL, date_format(RE.partition_date , 'dd-MM-yyyy'), date_format(RE.dt_ren_fec_altacto, 'dd-MM-yyyy')) fecapertura,
	IF(RE.dt_ren_fec_ven IS NULL,'01-01-9999',date_format(RE.dt_ren_fec_ven, 'dd-MM-yyyy')) fecvcto,
	RE.cod_ren_entidad_espana identidadmis,
	RE.cod_ren_centrocont idcentro,
	RE.cod_ren_subprodu idsubproductooperacional,
	'' zona,
	'' territorial,
	IF(RE.cod_ren_gestorprod = 'N/A',NULL,RE.cod_ren_gestorprod) gestorproducto,
	IF(RE.cod_ren_gestor = 'null',NULL,	RE.cod_ren_gestor) gestorcliente,
	SUM(RE.fc_ren_sdbsfm) sdbsfm,
	'' sfbsfm,
	SUM(RE.fc_ren_interes) intereses,
	SUM(RE.fc_ren_comfin) comfin,
	SUM(RE.fc_ren_comnofin) comnofin,
	IF(RE.cod_ren_vincula = 'null',0,RE.cod_ren_vincula) codvincula,
	SUM(RE.fc_ren_rof) rof,
	MAX(RE.fc_ren_tti) tti,
	'' mtm,
	'' nocional,
	RE.cod_ren_divisa_mis coddivmis,
	RE.flag_ren_moralocal flagmoralocal,
	RE.flag_ren_carterizadas flagcarterizadas,
	RE.ds_ren_nombrecliente nombreclien,
	COALESCE(RE.cod_per_tipopersona,'F') tippers,
	'' nifcif,
	IF(RE.ds_ren_intragrupo = 'OPERACIONAL',1,0) intragrupo,
	RE.cod_ren_internegocio internegocio,
	RE.cod_ren_areanegociocorp areadenegociocorp,
	RE.cod_productogest idproductogestion,
	RE.cod_segmentogest segmentoclientemis,
	RE.cod_producto_operacional idproductooperacional,
	RE.ds_ren_ficheromis ficheromis,
	date_format(RE.dt_ren_fec_reestruc, 'dd-MM-yyyy') fecrenov,
	RE.dt_ren_fec_extrdatos fecextrdatosmis,
	RE.flag_ren_neteo flagneteo,
	SUM(RE.fc_ren_orex) orex,
	'' costesmis,
	RE.cod_nivel_operacion niveloperacion,
	'' nombregestorproducto,
	'' nombregestorcliente
FROM
	bi_corp_common.bt_ror_input_pasivo RE
WHERE
	partition_date = '$month_to'
GROUP BY
	RE.cod_ren_unidad,
	date_format(RE.partition_date , 'yyyyMM'),
	RE.cod_ren_finalidaddatos,
	RE.cod_ren_contrato_rorac,
	RE.cod_ren_areanegocio,
	RE.cod_ren_divisa,
	RE.cod_ren_division,
	RE.cod_ren_producto,
	RE.cod_ren_pers_ods,
	IF(RE.dt_ren_fec_altacto IS NULL, date_format(RE.partition_date , 'dd-MM-yyyy'), date_format(RE.dt_ren_fec_altacto, 'dd-MM-yyyy')),
	IF(RE.dt_ren_fec_ven IS NULL,'01-01-9999',date_format(RE.dt_ren_fec_ven, 'dd-MM-yyyy')),
	RE.cod_ren_entidad_espana,
	RE.cod_ren_centrocont,
	RE.cod_ren_subprodu,
	IF(RE.cod_ren_gestorprod = 'N/A',NULL,RE.cod_ren_gestorprod),
	IF(RE.cod_ren_gestor = 'null',NULL,RE.cod_ren_gestor),
	IF(RE.cod_ren_vincula = 'null',0,RE.cod_ren_vincula),
	RE.cod_ren_divisa_mis,
	RE.flag_ren_moralocal,
	RE.flag_ren_carterizadas,
	RE.ds_ren_nombrecliente,
	COALESCE(RE.cod_per_tipopersona,'F'),
	IF(RE.ds_ren_intragrupo = 'OPERACIONAL',1,0),
	RE.cod_ren_internegocio,
	RE.cod_ren_areanegociocorp,
	RE.cod_productogest,
	RE.cod_segmentogest,
	RE.cod_producto_operacional,
	RE.ds_ren_ficheromis,
	date_format(RE.dt_ren_fec_reestruc, 'dd-MM-yyyy'),
	RE.dt_ren_fec_extrdatos,
	RE.flag_ren_neteo,
	RE.cod_nivel_operacion ;
	*/

	--------------------------------------------------------------------------------------------
----------------------------------------------------------------------- CONTROL CARDINALIDAD
	-- SELECT 'C_A' perimetro , COUNT(1) FROM cto_contenido_a 
	--	UNION ALL SELECT 'C_P' perimetro , COUNT(1) FROM cto_contenido_p 
	--	UNION ALL SELECT 'P_A' perimetro , COUNT(1) FROM cto_producto_a 
	--	UNION ALL SELECT 'P_P' perimetro , COUNT(1) FROM cto_producto_p ;	
----------------------------------------------------------------------------------
----------------------------------------------------------------------- CONTROL SALDOS
SELECT partition_date, cod_ren_contrato, cod_ren_area_negocio , sum(fc_ren_resultado_total_real_ml) ComNoFin                                                                                                                                         
FROM cto_activo 
WHERE cod_ren_contenido = 'CCL'                                                                                  
    AND cod_ren_contrato = '-99100'  
    AND TRIM(cod_ren_area_negocio) = 'P2AGRCIT'
GROUP BY partition_date, cod_ren_contrato, cod_ren_area_negocio ;

--| partition_date|cod_ren_contrato|cod_ren_area_negocio|comnofin    |
--|---------------|----------------|--------------------|------------|
--| 2020-10-31    |-99100          |P2AGRCIT            |-307770.5681|


--| FEC_DATA|IDF_CTO_ODS|COD_AREA_NEGOCIO|COMNOFIN    |
--|---------|-----------|----------------|------------|
--| 20201031|-99100     |P2AGRCIT        |-307770.5681|





