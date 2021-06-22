

set hive.execution.engine=spark;
set spark.yarn.queue=root.dataeng;

DROP TABLE IF EXISTS cto_contenido ;
CREATE TEMPORARY TABLE cto_contenido AS
WITH 

result_filtered AS (

SELECT * FROM bi_corp_common.bt_ren_result RE
WHERE RE.partition_date = '2020-12-31'
AND RE.cod_ren_cta_resultados_niv_6 != '1073'

),

cto_act_pas AS (

	SELECT DISTINCT RE.cod_ren_contrato 
			, RE.cod_ren_contenido 
			, RE.cod_ren_contrato_rorac 
			, IF(RE.cod_ren_contenido IN ('PLZ','CTA'), 'P', IF(RE.cod_ren_contenido IN ('PRE','CRE','ARF','CCO','ROF'), 'A', NULL)) perimetro
			, RE.cod_ren_producto_niv_3 
			, RE.cod_ren_area_negocio 
			, RE.cod_ren_producto_gest 
	FROM result_filtered RE
	WHERE RE.partition_date = '2020-12-31'
	AND RE.cod_ren_contenido IN ('PRE','CRE','ARF','CCO','PLZ','CTA','COM','CTB','ROF','CCL')
	AND RE.cod_ren_contrato != '-99100' 
	ORDER BY RE.cod_ren_contrato , cod_ren_contenido , RE.cod_ren_contrato_rorac , RE.cod_ren_producto_gest , RE.cod_ren_area_negocio
), 

cto_act_pas_join AS (

	SELECT DISTINCT A.cod_ren_contrato , A.cod_ren_contenido , A.cod_ren_contrato_rorac , A.cod_ren_area_negocio , A.cod_ren_producto_gest , A.cod_ren_producto_niv_3 
		, IF(A.perimetro IS NULL, IF(A.cod_ren_producto_niv_3 = 'BG1', 'A'
				, IF(SUBSTRING(A.cod_ren_producto_gest, 1, 2) IN ('C+', 'R+'), 'A' 
				, IF(A.cod_ren_producto_niv_3 = 'BG2', 'P'
				, IF(SUBSTRING(A.cod_ren_producto_gest, 1, 2) IN ('C-', 'R-'), 'P'
				, IF(SUBSTRING(A.cod_ren_producto_gest, 1, 1) = 'O', 'P', B.perimetro ))))), A.perimetro) perimetro
	FROM cto_act_pas A
		LEFT JOIN cto_act_pas B 
			ON A.cod_ren_contrato = B.cod_ren_contrato
			AND B.perimetro IS NOT NULL 
)

	SELECT DISTINCT cod_ren_contrato , cod_ren_contenido , cod_ren_contrato_rorac , cod_ren_area_negocio , cod_ren_producto_gest , perimetro
	FROM cto_act_pas_join WHERE perimetro IS NOT NULL ;



DROP TABLE IF EXISTS cto_contenido_a ;
CREATE TEMPORARY TABLE cto_contenido_a AS
	SELECT DISTINCT cod_ren_contrato , cod_ren_contenido , cod_ren_contrato_rorac , cod_ren_area_negocio , cod_ren_producto_gest
	FROM cto_contenido WHERE perimetro = 'A' ;

DROP TABLE IF EXISTS cto_contenido_p ;
CREATE TEMPORARY TABLE cto_contenido_p AS
	SELECT DISTINCT cod_ren_contrato , cod_ren_contenido , cod_ren_contrato_rorac , cod_ren_area_negocio , cod_ren_producto_gest
	FROM cto_contenido WHERE perimetro = 'P' ;




DROP TABLE IF EXISTS cto_producto ;
CREATE TEMPORARY TABLE cto_producto AS
WITH
pro_act_pas AS (

	SELECT DISTINCT RE.cod_ren_contrato 
			, RE.cod_ren_contenido 
			, RE.cod_ren_contrato_rorac 
			, IF(RE.cod_ren_producto_niv_3 = 'BG1', 'A'
				, IF(RE.cod_ren_producto_niv_3 = 'BG2', 'P'
				, IF(SUBSTRING(RE.cod_ren_producto_gest, 1, 2) IN ('C+', 'R+'), 'A'
				, IF(SUBSTRING(RE.cod_ren_producto_gest, 1, 2) IN ('C-', 'R-'), 'P'
				, IF(SUBSTRING(RE.cod_ren_producto_gest, 1, 1) = 'O', 'P', NULL ))))) perimetro 
			, RE.cod_ren_producto_gest
			, RE.cod_ren_area_negocio 
	FROM bi_corp_common.bt_ren_result RE
	WHERE RE.partition_date = '2020-12-31'
	AND RE.cod_ren_contenido IN ('COM','CTB','CCL') 
	AND RE.cod_ren_contrato = '-99100'
	AND RE.cod_ren_cta_resultados_niv_6 != '1073' 
)

	SELECT cod_ren_contrato , cod_ren_contenido , cod_ren_contrato_rorac  , perimetro , cod_ren_producto_gest , cod_ren_area_negocio
	FROM pro_act_pas  ;


DROP TABLE IF EXISTS cto_producto_a ;
CREATE TEMPORARY TABLE cto_producto_a AS
	SELECT DISTINCT cod_ren_contrato , cod_ren_contenido , cod_ren_contrato_rorac , cod_ren_producto_gest , cod_ren_area_negocio
	FROM cto_producto WHERE perimetro = 'A' ;

DROP TABLE IF EXISTS cto_producto_p ;
CREATE TEMPORARY TABLE cto_producto_p AS
	SELECT DISTINCT cod_ren_contrato , cod_ren_contenido , cod_ren_contrato_rorac , cod_ren_producto_gest , cod_ren_area_negocio
	FROM cto_producto WHERE perimetro = 'P' ; 




DROP TABLE IF EXISTS zror_perimetro_activo ;
CREATE TEMPORARY TABLE zror_perimetro_activo AS
AS SELECT DISTINCT RE.cod_ren_contrato
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
		, RE.fc_ren_resultado_total_ml 
		, RE.fc_ren_saldo_medio_ml 
		, RE.fc_ren_sdo_med_cap_ml
		, RE.fc_ren_sdo_med_int_ml 
		, RE.fc_ren_sdo_med_reajuste_ml 
		, RE.fc_ren_sdo_med_insolv_ml 
		, RE.fc_ren_ing_per_ml 
		, RE.fc_ren_ing_cap_ml 
		, RE.fc_ren_resultado_total_real_ml
		, RE.fc_ren_resultado_total_ficticio_ml
		, RE.partition_date 
	FROM bi_corp_common.bt_ren_result RE , cto_contenido_a CA 
	WHERE RE.partition_date = '2020-12-31'
		AND RE.cod_ren_contrato = CA.cod_ren_contrato
		AND RE.cod_ren_contenido = CA.cod_ren_contenido
		AND RE.cod_ren_contrato_rorac = CA.cod_ren_contrato_rorac
		AND RE.cod_ren_contenido IN ('PRE','CRE','ARF','CCO','COM','CTB','ROF','CCL')
		AND RE.cod_ren_area_negocio = CA.cod_ren_area_negocio
		AND RE.cod_ren_producto_gest = CA.cod_ren_producto_gest
		AND RE.cod_ren_contrato != '-99100' 
		AND RE.cod_ren_cta_resultados_niv_6 != '1073' ;
		
	INSERT INTO bi_corp_staging.zror_perimetro_activo
	SELECT DISTINCT RE.cod_ren_contrato 
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
		, RE.fc_ren_resultado_total_ml 
		, RE.fc_ren_saldo_medio_ml 
		, RE.fc_ren_sdo_med_cap_ml
		, RE.fc_ren_sdo_med_int_ml 
		, RE.fc_ren_sdo_med_reajuste_ml 
		, RE.fc_ren_sdo_med_insolv_ml 
		, RE.fc_ren_ing_per_ml 
		, RE.fc_ren_ing_cap_ml 
		, RE.fc_ren_resultado_total_real_ml
		, RE.fc_ren_resultado_total_ficticio_ml
		, RE.partition_date 
	FROM bi_corp_common.bt_ren_result RE , cto_producto_a PA 
	WHERE RE.partition_date = '2020-12-31'
		AND RE.cod_ren_contrato = PA.cod_ren_contrato
		AND RE.cod_ren_contenido = PA.cod_ren_contenido
		AND RE.cod_ren_producto_gest = PA.cod_ren_producto_gest
		AND RE.cod_ren_contenido IN ('COM','CTB','CCL')
		AND RE.cod_ren_area_negocio = PA.cod_ren_area_negocio
		AND RE.cod_ren_producto_gest = PA.cod_ren_producto_gest
		AND RE.cod_ren_contrato = '-99100' 
		AND RE.cod_ren_cta_resultados_niv_6 != '1073' ;

		



DROP TABLE IF EXISTS zror_perimetro_pasivo ;
CREATE TEMPORARY TABLE zror_perimetro_pasivo AS 
SELECT DISTINCT RE.cod_ren_contrato
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
		, RE.fc_ren_resultado_total_ml 
		, RE.fc_ren_saldo_medio_ml 
		, RE.fc_ren_sdo_med_cap_ml
		, RE.fc_ren_sdo_med_int_ml 
		, RE.fc_ren_sdo_med_reajuste_ml 
		, RE.fc_ren_sdo_med_insolv_ml 
		, RE.fc_ren_ing_per_ml 
		, RE.fc_ren_ing_cap_ml 
		, RE.fc_ren_resultado_total_real_ml
		, RE.fc_ren_resultado_total_ficticio_ml
		, RE.partition_date 
	FROM bi_corp_common.bt_ren_result RE , cto_contenido_p CP 
	WHERE RE.partition_date = '2020-12-31'
		AND RE.cod_ren_contrato = CP.cod_ren_contrato
		AND RE.cod_ren_contenido = CP.cod_ren_contenido
		AND RE.cod_ren_contrato_rorac = CP.cod_ren_contrato_rorac
		AND RE.cod_ren_contenido IN ('PLZ','CTA','COM','CTB','CCL')
		AND RE.cod_ren_contrato != '-99100' 
		AND RE.cod_ren_area_negocio = CP.cod_ren_area_negocio 
		AND RE.cod_ren_producto_gest = CP.cod_ren_producto_gest 
		AND RE.cod_ren_cta_resultados_niv_6 != '1073' ;
	
	INSERT INTO zror_perimetro_pasivo
	SELECT DISTINCT RE.cod_ren_contrato 
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
		, RE.fc_ren_resultado_total_ml 
		, RE.fc_ren_saldo_medio_ml 
		, RE.fc_ren_sdo_med_cap_ml
		, RE.fc_ren_sdo_med_int_ml 
		, RE.fc_ren_sdo_med_reajuste_ml 
		, RE.fc_ren_sdo_med_insolv_ml 
		, RE.fc_ren_ing_per_ml 
		, RE.fc_ren_ing_cap_ml 
		, RE.fc_ren_resultado_total_real_ml
		, RE.fc_ren_resultado_total_ficticio_ml
		, RE.partition_date 
	FROM bi_corp_common.bt_ren_result RE , cto_producto_p PP 
	WHERE RE.partition_date = '2020-12-31'
		AND RE.cod_ren_contrato = PP.cod_ren_contrato
		AND RE.cod_ren_contenido = PP.cod_ren_contenido
		AND RE.cod_ren_producto_gest = PP.cod_ren_producto_gest
		AND RE.cod_ren_contenido IN ('COM','CTB','CCL')
		AND RE.cod_ren_contrato = '-99100' 
		AND RE.cod_ren_producto_gest = PP.cod_ren_producto_gest
		AND RE.cod_ren_area_negocio = PP.cod_ren_area_negocio 
		AND RE.cod_ren_cta_resultados_niv_6 != '1073' ;

	



DROP TABLE IF EXISTS zror_producto_mis ;
CREATE TEMPORARY TABLE  zror_producto_mis AS
	SELECT TRIM(cod_valor) cod_producto_gest
		, TRIM(cod_valor_padre) cod_producto_mis
		, TRIM(num_nivel) num_nivel
	FROM bi_corp_staging.rio157_ms0_dm_dwh_composicion_jerqs 
	WHERE partition_date = '2020-12-15'
		AND cod_jerarquia = 'JBP01'
		AND cod_dimension = 'PR01' ;



DROP TABLE IF EXISTS zror_tablon_ifrs9 ;
CREATE TEMPORARY TABLE zror_tablon_ifrs9 AS  
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
	WHERE RI.periodo = '20201231'
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



DROP TABLE IF EXISTS zror_rosetta_key ;
CREATE TEMPORARY TABLE zror_rosetta_key AS
	SELECT DISTINCT MK.native_key AS cod_ren_contrato_rorac
		, BDR.native_key AS id_cto_bdr
	FROM bi_corp_common.rosetta_nkey_hist MK
		JOIN bi_corp_common.rosetta_nkey_hist BDR ON 
			MK.master_key = BDR.master_key 
			AND BDR.partition_date = '2020-12-31'
			AND BDR.domain_code = '00004'
	WHERE MK.partition_date = '2020-12-31'
		AND MK.domain_code = '00005';




	DROP TABLE IF EXISTS zror_activo_01 ;
	CREATE TEMPORARY TABLE zror_activo_01 AS
	AS SELECT RE.*
		, adn2.cod_ren_area_negocio_niv_2 cod_ren_division
		, pm.cod_producto_mis cod_ren_producto
		, adn.cod_ren_primercorporativo  cod_ren_area_negociocorp 
	FROM zror_perimetro_activo RE  

	LEFT JOIN bi_corp_common.dim_ren_areanegocioctr adn2 ON 
		RE.cod_ren_area_negocio = adn2.cod_ren_area_negocio_niv_9 

	LEFT JOIN zror_producto_mis pm ON 
		RE.cod_productogest = PM.cod_producto_gest

	LEFT JOIN bi_corp_common.dim_ren_jeareanegocioctr adn ON 
		RE.cod_ren_area_negocio = adn.cod_ren_areanegociohijo ;
	



	DROP TABLE IF EXISTS zror_activo_02 ;
	CREATE TEMPORARY TABLE zror_activo_02 AS
	AS SELECT RE.*
		, RK.id_cto_bdr		
		, ROW_NUMBER() OVER(PARTITION BY SUBSTRING(RE.cod_ren_contrato, 1, 26)) int_ren_ordercontrato
	FROM zror_activo_01 RE

	LEFT JOIN zror_rosetta_key RK ON 
		RE.cod_ren_contrato_rorac = RK.cod_ren_contrato_rorac 
	WHERE RE.cod_ren_contrato != '-99100' ;

	INSERT INTO zror_activo_02
	SELECT DISTINCT RE.*
		, NULL id_cto_bdr
		, NULL int_ren_ordercontrato
	FROM zror_activo_01 RE
	WHERE RE.cod_ren_contrato = '-99100' ;

	
	DROP TABLE IF EXISTS zror_activo ;
	CREATE TEMPORARY TABLE zror_activo AS
	AS SELECT RE.*
		, RI.s_provision_amount fc_ifrs_provision
	FROM zror_activo_02 RE

	LEFT JOIN zror_tablon_ifrs9 RI ON 
		(SUBSTRING(RE.cod_ren_contrato,1,26) = RI.cod_ren_contrato_short
		and RE.int_ren_ordercontrato = 1) 
	WHERE RE.cod_ren_contrato != '-99100' ;

	INSERT INTO zror_activo
	SELECT DISTINCT RE.*
		, NULL fc_ifrs_provision
	FROM zror_activo_02 RE
	WHERE RE.cod_ren_contrato = '-99100' ;
	



	DROP TABLE IF EXISTS zror_pasivo ;
	CREATE TEMPORARY TABLE zror_pasivo AS
	AS SELECT RE.*
		, adn2.cod_ren_area_negocio_niv_2 cod_ren_division
		, pm.cod_producto_mis cod_ren_producto
		, adn.cod_ren_primercorporativo  cod_ren_area_negociocorp 
		, 0 id_cto_bdr
		, 0 int_ren_ordercontrato
		, 0 fc_ifrs_provision
	FROM zror_perimetro_pasivo RE	 

	LEFT JOIN bi_corp_common.dim_ren_areanegocioctr adn2 ON 
		RE.cod_ren_area_negocio = adn2.cod_ren_area_negocio_niv_9 

	LEFT JOIN zror_producto_mis pm ON 
		RE.cod_productogest = PM.cod_producto_gest

	LEFT JOIN bi_corp_common.dim_ren_jeareanegocioctr adn ON 
		RE.cod_ren_area_negocio = adn.cod_ren_areanegociohijo ;
	



INSERT OVERWRITE TABLE bi_corp_common.bt_ror_input_activo
PARTITION(partition_date) 

	SELECT 'ARG' cod_ren_unidad ,
		date_format(partition_date, 'yyyyMM') dt_ren_data ,
		1 cod_ren_finalidaddatos ,
		IF(cod_ren_contenido IN ('CCL','CTB'), cod_ren_cliente_rorac, cod_ren_contrato_rorac) cod_ren_contrato_rorac ,
		cod_ren_contrato ,
		cod_ren_area_negocio cod_ren_area_negocio ,
		'ARS' cod_ren_divisa ,
		cod_ren_division ,
		cod_ren_producto ,
		cod_ren_pers_ods ,
		SUM(fc_ren_resultado_total_ml) fc_ren_margenmes ,
		SUM(fc_ren_saldo_medio_ml) fc_ren_sdbsmv ,
		CASE WHEN TRIM(cod_ren_producto_niv_3) = 'BG3' THEN (SUM(fc_ren_sdo_med_cap_ml)
											+ SUM(fc_ren_sdo_med_int_ml)
											+ SUM(fc_ren_sdo_med_reajuste_ml)
											+ SUM(fc_ren_sdo_med_insolv_ml)) ELSE 0 END fc_ren_sfbsmv ,
		dt_ren_altacontrato ,
		dt_ren_vencontrato ,
		cod_ren_entidad_espana ,
		cod_ren_centrocont ,
		cod_ren_subprodu ,
		NULL cod_ren_zona ,
		NULL cod_ren_territorial ,
		IF(TRIM(cod_ren_gestor_prod) = 'N/A', NULL, TRIM(cod_ren_gestor_prod)) cod_ren_gestor_prod ,
		cod_ren_gestor ,
		id_cto_bdr id_cto_bdr ,
		SUM(fc_ren_ing_per_ml)
			+ SUM(fc_ren_ing_cap_ml)
			+ SUM(fc_ren_sdo_med_cap_ml)
			+ SUM(fc_ren_sdo_med_int_ml)
			+ SUM(fc_ren_sdo_med_reajuste_ml)
			+ SUM(fc_ren_sdo_med_insolv_ml) fc_ren_sdbsfm ,
		CASE WHEN TRIM(cod_ren_cta_resultados_niv_9) = '1031' THEN SUM(fc_ren_resultado_total_real_ml) ELSE 0 END fc_ren_interes ,
		CASE WHEN TRIM(cod_ren_cta_resultados_niv_9) = '1032' THEN SUM(fc_ren_resultado_total_real_ml) ELSE 0 END fc_ren_comfin ,
		CASE WHEN TRIM(cod_ren_cta_resultados_niv_8) = '1051' THEN SUM(fc_ren_resultado_total_real_ml) ELSE 0 END fc_ren_comnofin ,
		cod_ren_vincula ,
		CASE WHEN TRIM(cod_ren_cta_resultados_niv_8) = '1052' THEN SUM(fc_ren_resultado_total_real_ml) ELSE 0 END fc_ren_rof ,
		SUM(fc_ren_resultado_total_ficticio_ml) fc_ren_tti ,
		NULL cod_ren_mtm ,
		NULL cod_ren_nocional ,
		cod_ren_divisa_mis ,
		NULL flag_ren_moralocal ,
		NULL flag_ren_carterizadas ,
		ds_ren_nombrecliente ,
		cod_per_tipopersona ,
		NULL cod_ren_nifcif ,
		ds_ren_intragrupo ,
		cod_ren_internegocio ,
		 cod_ren_area_negociocorp ,
		cod_productogest ,
		cod_segmentogest ,
		cod_producto_operacional ,
		CONCAT(regexp_replace(partition_date, '-', ''), 'v1') ds_ren_ficheromis ,
		dt_ren_reestruccontrato ,
		date_format(partition_date, 'dd-MM-yyyy') dt_ren_fec_extrdatos ,
		IF(cod_ren_contenido = 'CCL', 2, 0) flag_ren_neteo ,
		CASE WHEN TRIM(cod_ren_cta_resultados_niv_8) = '1054' THEN SUM(fc_ren_resultado_total_real_ml) ELSE 0 END fc_ren_orex ,
		SUM(fc_ifrs_provision) fc_ifrs_provision ,
		NULL fc_ren_costemensualcto ,
		IF(cod_ren_contenido NOT IN ('CCL','CTB'), 0, IF(cod_ren_pers_ods != '-99100', 1, 3)) cod_nivel_operacion ,
		NULL cod_ren_idru ,
		partition_date
	FROM zror_activo 
	GROUP BY date_format(partition_date, 'yyyyMM') ,
			IF(cod_ren_contenido IN ('CCL','CTB'), cod_ren_cliente_rorac, cod_ren_contrato_rorac) ,
			cod_ren_contrato ,
			cod_ren_area_negocio ,
			cod_ren_division ,
			cod_ren_producto ,
			cod_ren_pers_ods ,
			dt_ren_altacontrato ,
			dt_ren_vencontrato ,
			cod_ren_entidad_espana ,
			cod_ren_centrocont ,
			cod_ren_subprodu ,
			cod_ren_gestor_prod ,
			cod_ren_gestor ,
			id_cto_bdr ,
			cod_ren_vincula ,
			cod_ren_divisa_mis ,
			ds_ren_nombrecliente ,
			cod_per_tipopersona ,
			ds_ren_intragrupo ,
			cod_ren_internegocio ,
			cod_ren_area_negociocorp ,
			cod_productogest ,
			cod_segmentogest ,
			cod_producto_operacional ,
			CONCAT(regexp_replace(partition_date, '-', ''), 'v1') ,
			dt_ren_reestruccontrato ,
			date_format(partition_date, 'dd-MM-yyyy') ,
			IF(cod_ren_contenido = 'CCL', 2, 0) ,
			IF(cod_ren_contenido NOT IN ('CCL','CTB'), 0, IF(cod_ren_pers_ods != '-99100', 1, 3)) ,
			partition_date ,
			cod_ren_producto_niv_3 ,
			cod_ren_cta_resultados_niv_8 ,
			cod_ren_cta_resultados_niv_9 
	HAVING SUM(fc_ren_resultado_total_ml) != 0
			OR (CASE WHEN TRIM(cod_ren_producto_niv_3) = 'BG3' THEN (SUM(fc_ren_sdo_med_cap_ml)
											+ SUM(fc_ren_sdo_med_int_ml)
											+ SUM(fc_ren_sdo_med_reajuste_ml)
											+ SUM(fc_ren_sdo_med_insolv_ml)) ELSE 0 END != 0 )
			OR (SUM(fc_ren_saldo_medio_ml) != 0) ;




INSERT OVERWRITE TABLE bi_corp_common.bt_ror_input_pasivo
PARTITION(partition_date) 

	SELECT 'ARG' cod_ren_unidad ,
		date_format(partition_date, 'yyyyMM') dt_ren_data ,
		1 cod_ren_finalidaddatos ,
		IF(cod_ren_contenido IN ('CCL','CTB'), cod_ren_cliente_rorac, cod_ren_contrato_rorac) cod_ren_contrato_rorac ,
		cod_ren_contrato ,
		cod_ren_area_negocio ,
		'ARS' cod_ren_divisa ,
		cod_ren_division ,
		cod_ren_producto ,
		cod_ren_pers_ods ,
		SUM(fc_ren_resultado_total_ml) fc_ren_margenmes ,
		SUM(fc_ren_saldo_medio_ml) fc_ren_sdbsmv ,
		CASE WHEN TRIM(cod_ren_producto_niv_3) = 'BG3' THEN (SUM(fc_ren_sdo_med_cap_ml)
											+ SUM(fc_ren_sdo_med_int_ml)
											+ SUM(fc_ren_sdo_med_reajuste_ml)
											+ SUM(fc_ren_sdo_med_insolv_ml)) ELSE 0 END fc_ren_sfbsmv ,
		dt_ren_altacontrato ,
		dt_ren_vencontrato ,
		cod_ren_entidad_espana ,
		cod_ren_centrocont ,
		cod_ren_subprodu ,
		NULL cod_ren_zona ,
		NULL cod_ren_territorial ,
		IF(TRIM(cod_ren_gestor_prod) = 'N/A', NULL, TRIM(cod_ren_gestor_prod)) cod_ren_gestor_prod ,
		cod_ren_gestor ,
		NULL id_cto_bdr ,
		(SUM(fc_ren_ing_per_ml)
			+ SUM(fc_ren_ing_cap_ml)
			+ SUM(fc_ren_sdo_med_cap_ml)
			+ SUM(fc_ren_sdo_med_int_ml)
			+ SUM(fc_ren_sdo_med_reajuste_ml)
			+ SUM(fc_ren_sdo_med_insolv_ml)) fc_ren_sdbsfm ,
		CASE WHEN cod_ren_cta_resultados_niv_9 = '1031' THEN SUM(fc_ren_resultado_total_real_ml) ELSE 0 END fc_ren_interes ,
		CASE WHEN cod_ren_cta_resultados_niv_9 = '1032' THEN SUM(fc_ren_resultado_total_real_ml) ELSE 0 END fc_ren_comfin ,
		CASE WHEN cod_ren_cta_resultados_niv_8 = '1051' THEN SUM(fc_ren_resultado_total_real_ml) ELSE 0 END fc_ren_comnofin ,
		cod_ren_vincula ,
		CASE WHEN cod_ren_cta_resultados_niv_8 = '1052' THEN SUM(fc_ren_resultado_total_real_ml) ELSE 0 END fc_ren_rof ,
		SUM(fc_ren_resultado_total_ficticio_ml) fc_ren_tti ,
		NULL cod_ren_mtm ,
		NULL cod_ren_nocional ,
		cod_ren_divisa_mis ,
		NULL flag_ren_moralocal ,
		NULL flag_ren_carterizadas ,
		ds_ren_nombrecliente ,
		cod_per_tipopersona ,
		NULL cod_ren_nifcif ,
		ds_ren_intragrupo ,
		cod_ren_internegocio ,
		cod_ren_area_negociocorp ,
		cod_productogest ,
		cod_segmentogest ,
		cod_producto_operacional ,
		CONCAT(regexp_replace(partition_date, '-', ''), 'v1') ds_ren_ficheromis ,
		dt_ren_reestruccontrato ,
		date_format(partition_date, 'dd-MM-yyyy') dt_ren_fec_extrdatos ,
		IF(cod_ren_contenido = 'CCL', 2, 0) flag_ren_neteo ,
		CASE WHEN cod_ren_cta_resultados_niv_8 = '1054' THEN SUM(fc_ren_resultado_total_real_ml) ELSE 0 END fc_ren_orex ,
		NULL fc_ifrs_provision ,
		NULL fc_ren_costemensualcto ,
		IF(cod_ren_contenido NOT IN ('CCL','CTB'), 0, IF(cod_ren_pers_ods != '-99100', 1, 3)) cod_nivel_operacion ,
		partition_date
	FROM zror_pasivo
	GROUP BY cod_ren_contenido ,
			cod_ren_cliente_rorac, 
			cod_ren_contrato_rorac ,
			cod_ren_contrato ,
			cod_ren_area_negocio ,
			cod_ren_division ,
			cod_ren_producto ,
			cod_ren_pers_ods ,
			dt_ren_altacontrato ,
			dt_ren_vencontrato ,
			cod_ren_entidad_espana ,
			cod_ren_centrocont ,
			cod_ren_subprodu ,
			cod_ren_gestor_prod ,
			cod_ren_gestor ,
			cod_ren_vincula ,
			cod_ren_divisa_mis ,
			ds_ren_nombrecliente ,
			cod_per_tipopersona ,
			ds_ren_intragrupo ,
			cod_ren_internegocio ,
			cod_ren_area_negociocorp ,
			cod_productogest ,
			cod_segmentogest ,
			cod_producto_operacional ,
			dt_ren_reestruccontrato ,
			cod_ren_cta_resultados_niv_8 ,
			cod_ren_cta_resultados_niv_9 ,
			cod_ren_producto_niv_3 ,
			cod_ren_pers_ods ,
			partition_date 
	HAVING SUM(fc_ren_resultado_total_ml) != 0 ;
	
--
--
--
--
--

DROP TABLE IF EXISTS  ru ; 
SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;
CREATE TEMPORARY TABLE ru AS
 WITH rs_activo AS (
	SELECT DISTINCT RE.cod_ren_pers_ods
		, RE.cod_producto_operacional cod_producto
		, RE.cod_ren_subprodu cod_subprodu
		, RE.cod_segmentogest
		, RE.cod_ren_divisa_mis
	FROM bi_corp_common.bt_ror_input_activo RE
	WHERE RE.partition_date = '2020-12-31' 
	AND TRIM(RE.cod_ren_pers_ods) != '-99100'
	AND RE.cod_ren_subprodu IS NOT NULL
	)
    , rs_segmento AS (
	SELECT RE.cod_ren_pers_ods
		, RE.cod_producto
		, RE.cod_subprodu
		, RE.cod_segmentogest
		, RE.cod_ren_divisa_mis
		, TRIM(SE.pesegcal) pesegcal
	FROM rs_activo RE
	LEFT JOIN 
		(SELECT DISTINCT penumper 
			, pesegcal
		FROM bi_corp_staging.malpe_pedt030
		WHERE 	partition_date = '2020-12-30' ) SE
		ON regexp_replace(RE.cod_ren_pers_ods, '10072', '') = SE.penumper
	)
    , rs_baremo AS ( 	 
    SELECT RE.*
    	, BA.cod_baremo_local
        , BG.cod_baremo_global
    FROM rs_segmento RE
    LEFT JOIN bi_corp_bdr.v_baremos_local BA
        ON TRIM(BA.cod_baremo_alfanumerico_local) = TRIM(concat(RE.cod_producto, RE.cod_subprodu))
        AND BA.cod_negocio_local = '3'
    LEFT JOIN bi_corp_bdr.v_map_baremos_global_local BG
        ON BG.cod_baremo_local = BA.cod_baremo_local 
    WHERE BA.cod_baremo_local IS NOT NULL 
    )
    SELECT DISTINCT cod_ren_pers_ods
        , CASE WHEN pesegcal IN ("A", "B", "C", "S") AND cod_baremo_global = "0006" THEN 1100711 
	         WHEN pesegcal IN ("A", "B", "C", "S") AND cod_baremo_global IN ("0013", "0182", "0183") THEN 1100720
	         WHEN pesegcal IN ("A", "B", "C", "S") AND cod_baremo_global = "0017" THEN 1100714
	         WHEN pesegcal IN ("A", "B", "C", "S") AND cod_baremo_global NOT IN ("0017", "0006", "0013", "0182", "0183") THEN 1100717
	         WHEN pesegcal IN ("P1", "C1") THEN 1100709
	         WHEN pesegcal = "P2" THEN 1100540
	         WHEN pesegcal IN ("EM", "G1", "G2") THEN 1100724
	         WHEN pesegcal IN ("IU", "IP") THEN 1100722
	         WHEN cod_baremo_local = 1 AND pesegcal IN ("F2", "OF", "GL", "LA", "LO", "MU", "OT", "FI", "F1") THEN 1040176
	         WHEN cod_baremo_local = 11 AND pesegcal IN ("F2", "OF", "GL", "LA", "LO", "MU", "OT", "FI", "F1") THEN	1040178
	         WHEN cod_baremo_local = 17 AND pesegcal IN ("F2", "OF", "GL", "LA", "LO", "MU", "OT", "FI", "F1") THEN	1040179
	         WHEN pesegcal = "PU" AND cod_ren_divisa_mis = "ARS"  THEN 1200354
	         WHEN pesegcal = "PU" AND cod_ren_divisa_mis <> "ARS"  THEN 1040181 ELSE NULL END id_ru 
FROM rs_baremo ;


DROP TABLE IF EXISTS activo_with_ru ;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;
CREATE TEMPORARY TABLE activo_with_ru AS
SELECT RE.cod_ren_unidad
	, RE.dt_ren_data
	, RE.cod_ren_finalidaddatos
	, RE.cod_ren_contrato_rorac
	, RE.cod_ren_contrato
	, RE.cod_ren_areanegocio
	, RE.cod_ren_divisa
	, RE.cod_ren_division
	, RE.cod_ren_producto
	, RE.cod_ren_pers_ods
	, RE.fc_ren_margenmes
	, RE.fc_ren_sdbsmv
	, RE.fc_ren_sfbsmv
	, RE.dt_ren_fec_altacto
	, RE.dt_ren_fec_ven
	, RE.cod_ren_entidad_espana
	, RE.cod_ren_centrocont
	, RE.cod_ren_subprodu
	, RE.cod_ren_zona
	, RE.cod_ren_territorial
	, RE.cod_ren_gestorprod
	, RE.cod_ren_gestor
	, RE.id_cto_bdr
	, RE.fc_ren_sdbsfm
	, RE.fc_ren_interes
	, RE.fc_ren_comfin
	, RE.fc_ren_comnofin
	, RE.cod_ren_vincula
	, RE.fc_ren_rof
	, RE.fc_ren_tti
	, RE.cod_ren_mtm
	, RE.cod_ren_nocional
	, RE.cod_ren_divisa_mis
	, RE.flag_ren_moralocal
	, RE.flag_ren_carterizadas
	, RE.ds_ren_nombrecliente
	, RE.cod_per_tipopersona
	, RE.cod_ren_nifcif
	, RE.ds_ren_intragrupo
	, RE.cod_ren_internegocio
	, RE.cod_ren_areanegociocorp
	, RE.cod_productogest
	, RE.cod_segmentogest
	, RE.cod_producto_operacional
	, RE.ds_ren_ficheromis
	, RE.dt_ren_fec_reestruc
	, RE.dt_ren_fec_extrdatos
	, RE.flag_ren_neteo
	, RE.fc_ren_orex
	, RE.fc_ifrs_provision
	, RE.fc_ren_costemensualcto
	, RE.cod_nivel_operacion
	, RU.id_ru cod_ren_idru
	, RE.partition_date
FROM bi_corp_common.bt_ror_input_activo RE
LEFT JOIN ru RU 
	ON RU.cod_ren_pers_ods = RE.cod_ren_pers_ods
WHERE RE.partition_date = '2020-12-31' ;




SELECT COUNT(1) FROM (SELECT DISTINCT * FROM activo_with_ru)A ; -- 143.731.779 -- 143.731.830


SET hive.exec.dynamic.partition.mode=nonstrict;
SET mapred.job.queue.name=root.dataeng;
INSERT OVERWRITE TABLE bi_corp_common.bt_ror_input_activo
PARTITION(partition_date)
SELECT DISTINCT * FROM activo_with_ru ;


---------------------------------------

SELECT * FROM bi_corp_staging.rio157_ms0_dt_dwh_generic_result 
WHERE partition_date = '2020-12-31' LIMIT 10 ;
AND SUBSTRING(RE.cod_ren_contrato,11,4) != co

DESCRIBE bi_corp_staging.rio157_ms0_dt_dwh_generic_result 
















