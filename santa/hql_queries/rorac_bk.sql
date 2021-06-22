

-- CONTRATOS DICIEMBRE 2020:

set hive.execution.engine=spark ;
set spark.yarn.queue=root.dataeng ;
set hive.exec.dynamic.partition.mode=nonstrict ;

DROP TABLE IF EXISTS contratos_s2 ;
CREATE TEMPORARY TABLE contratos_s2 AS 
WITH max_margen_resultado AS (

	SELECT cod_ren_contenido
		, cod_ren_contrato
		, cod_ren_area_negocio
		, MAX(fc_ren_resultado_total_ml) max_margenmes
		, MAX(IF(cod_ren_contenido IN ('PLZ','CTA'), 'P', IF(cod_ren_contenido IN ('PRE','CRE','ARF','CCO','ROF'), 'A', NULL)))  perimetro
	FROM bi_corp_common.bt_ren_result
	WHERE partition_date = '2020-12-31' 
		AND cod_ren_contenido IN ('PRE','CRE','ARF','CCO','PLZ','CTA','COM','CTB','ROF','CCL')
		AND cod_ren_cta_resultados_niv_6 NOT IN ('1073', '1074', '1075')
		AND cod_ren_cta_resultados_niv_3 NOT IN ('99000', '1091')
		AND cod_ren_cta_resultados_niv_7 != '1064'
		AND cod_ren_cta_resultados_niv_5 != '1081'
		AND cod_ren_cta_resultados_niv_8 NOT IN ('104', '1050') 
	GROUP BY cod_ren_contenido
		, cod_ren_contrato
		, cod_ren_area_negocio
	
)

SELECT mr.cod_ren_contenido
	, mr.cod_ren_contrato
	, mr.cod_ren_area_negocio
	, mr.max_margenmes
	, max(br.cod_ren_producto_gest) cod_ren_producto_gest
	, max(mr.perimetro) perimetro
	, br.partition_date
FROM max_margen_resultado mr , bi_corp_common.bt_ren_result br
WHERE br.partition_date = '2020-12-31' 
	AND mr.cod_ren_contenido = br.cod_ren_contenido
	AND mr.cod_ren_contrato = br.cod_ren_contrato
	AND mr.cod_ren_area_negocio = br.cod_ren_area_negocio
	AND mr.max_margenmes = br.fc_ren_resultado_total_ml
	AND br.cod_ren_contenido IN ('PRE','CRE','ARF','CCO','PLZ','CTA','COM','CTB','ROF','CCL')
	AND br.cod_ren_cta_resultados_niv_6 NOT IN ('1073', '1074', '1075')
	AND br.cod_ren_cta_resultados_niv_3 NOT IN ('99000', '1091')
	AND br.cod_ren_cta_resultados_niv_7 != '1064'
	AND br.cod_ren_cta_resultados_niv_5 != '1081'
	AND br.cod_ren_cta_resultados_niv_8 NOT IN ('104', '1050') 
	AND max_margenmes != 0
GROUP BY mr.cod_ren_contenido
	, mr.cod_ren_contrato
	, mr.cod_ren_area_negocio
	, mr.max_margenmes 
	, br.partition_date ;


DROP TABLE IF EXISTS contratos_s3 ;
CREATE TEMPORARY TABLE contratos_s3 AS 
WITH result_saldos AS (
	SELECT cod_ren_contenido
	    , cod_ren_contrato
		, cod_ren_area_negocio
	    , SUM(COALESCE(fc_ren_resultado_total_ml, 0)) fc_ren_margenmes
		, SUM(COALESCE(fc_ren_saldo_medio_ml, 0)) fc_ren_sdbsmv 
		, CASE WHEN TRIM(cod_ren_producto_niv_3) = 'BG3' THEN (SUM(COALESCE(fc_ren_sdo_med_cap_ml, 0))
															+ SUM(COALESCE(fc_ren_sdo_med_int_ml, 0))
															+ SUM(COALESCE(fc_ren_sdo_med_reajuste_ml, 0))
															+ SUM(COALESCE(fc_ren_sdo_med_insolv_ml, 0))) ELSE 0 END fc_ren_sfbsmv
		, (SUM(fc_ren_ing_per_ml)
			+ SUM(fc_ren_ing_cap_ml)
			+ SUM(fc_ren_sdo_med_cap_ml)
			+ SUM(fc_ren_sdo_med_int_ml)
			+ SUM(fc_ren_sdo_med_reajuste_ml)
			+ SUM(fc_ren_sdo_med_insolv_ml)) fc_ren_sdbsfm 
		, CASE WHEN cod_ren_cta_resultados_niv_9 = '1031' THEN SUM(COALESCE(fc_ren_resultado_total_real_ml, 0)) ELSE 0 END fc_ren_interes 
		, CASE WHEN cod_ren_cta_resultados_niv_9 = '1032' THEN SUM(COALESCE(fc_ren_resultado_total_real_ml, 0)) ELSE 0 END fc_ren_comfin 
		, CASE WHEN cod_ren_cta_resultados_niv_8 = '1051' THEN SUM(COALESCE(fc_ren_resultado_total_real_ml, 0)) ELSE 0 END fc_ren_comnofin 
		, CASE WHEN cod_ren_cta_resultados_niv_8 = '1052' THEN SUM(COALESCE(fc_ren_resultado_total_real_ml, 0)) ELSE 0 END fc_ren_rof
		, SUM(COALESCE(fc_ren_resultado_total_ficticio_ml, 0)) fc_ren_tti
		, CASE WHEN cod_ren_cta_resultados_niv_8 = '1054' THEN SUM(COALESCE(fc_ren_resultado_total_real_ml, 0)) ELSE 0 END fc_ren_orex
		, IF(cod_ren_producto_niv_3 = 'BG1', 'A', IF(cod_ren_producto_niv_3 = 'BG2', 'P', NULL)) perimetro
	FROM bi_corp_common.bt_ren_result WHERE partition_date = '2020-12-31'
		AND cod_ren_cta_resultados_niv_6 NOT IN ('1073', '1074', '1075')
		AND cod_ren_cta_resultados_niv_3 NOT IN ('99000', '1091')
		AND cod_ren_cta_resultados_niv_7 != '1064'
		AND cod_ren_cta_resultados_niv_5 != '1081'
		AND cod_ren_cta_resultados_niv_8 NOT IN ('104', '1050') 
	GROUP BY cod_ren_contenido
	    , cod_ren_contrato
		, cod_ren_area_negocio
		, cod_ren_cta_resultados_niv_8
		, cod_ren_cta_resultados_niv_9
		, cod_ren_producto_niv_3
	HAVING sum(fc_ren_resultado_total_ml) != 0 
)

SELECT s2.cod_ren_contenido
	, s2.cod_ren_contrato
	, s2.cod_ren_area_negocio
	, MAX(s2.cod_ren_producto_gest) cod_ren_producto_gest
	, SUM(fc_ren_margenmes) fc_ren_margenmes
	, SUM(fc_ren_sdbsmv) fc_ren_sdbsmv
	, SUM(fc_ren_sfbsmv) fc_ren_sfbsmv
	, SUM(IF(SUBSTRING(cod_ren_producto_gest, 1, 2) IN ('C+', 'R+', 'C-', 'R-') 
		OR SUBSTRING(cod_ren_producto_gest, 1, 1) = 'O', 0, fc_ren_sdbsfm)) fc_ren_sdbsfm -- dentro fuera balance --  
	, SUM(fc_ren_interes) fc_ren_interes
	, SUM(fc_ren_comfin) fc_ren_comfin
	, SUM(fc_ren_comnofin) fc_ren_comnofin
	, SUM(fc_ren_rof) fc_ren_rof
	, SUM(fc_ren_tti) fc_ren_tti
	, SUM(fc_ren_orex) fc_ren_orex
	, MAX(COALESCE(s2.perimetro, re.perimetro)) perimetro
	, s2.partition_date
FROM contratos_s2 s2
JOIN result_saldos re 
	ON re.cod_ren_contenido = s2.cod_ren_contenido
	AND re.cod_ren_contrato = s2.cod_ren_contrato
	AND re.cod_ren_area_negocio = s2.cod_ren_area_negocio 
GROUP BY s2.cod_ren_contenido
	, s2.cod_ren_contrato
	, s2.cod_ren_area_negocio
	, s2.partition_date ;

DROP TABLE IF EXISTS contratos_s4 ;
CREATE TEMPORARY TABLE contratos_s4 AS 
WITH attr_result AS (
	SELECT DISTINCT cod_ren_contenido
			    , cod_ren_contrato
				, cod_ren_area_negocio
				, cod_ren_producto_gest
				, cod_ren_pers_ods
				, dt_ren_altacontrato
				, dt_ren_vencontrato
				, dt_ren_reestruccontrato
				, cod_ren_entidad_espana
				, cod_ren_centro_cont
				, cod_ren_gestor_prod
				, cod_ren_gestor
				, cod_ren_vincula
				, cod_ren_divisa cod_ren_divisa_mis
				, ds_per_nombre_apellido
				, cod_per_tipopersona
				, cod_ren_origen_inf
				, CONCAT(TRIM(cod_ren_tipo_ajuste),TRIM(cod_ren_origen_inf)) cod_ren_internegocio
				, cod_ren_segmento_gest	
		    FROM bi_corp_common.bt_ren_result WHERE partition_date = '2020-12-31'
			AND cod_ren_cta_resultados_niv_6 NOT IN ('1073', '1074', '1075')
			AND cod_ren_cta_resultados_niv_3 NOT IN ('99000', '1091')
			AND cod_ren_cta_resultados_niv_7 != '1064'
			AND cod_ren_cta_resultados_niv_5 != '1081'
			AND cod_ren_cta_resultados_niv_8 NOT IN ('104', '1050') 

)

SELECT s3.cod_ren_contenido	
	, s3.cod_ren_contrato	
	, s3.cod_ren_area_negocio	
	, MAX(s3.cod_ren_producto_gest)	cod_ren_producto_gest
	, MAX(s3.fc_ren_margenmes) fc_ren_margenmes
	, MAX(s3.fc_ren_sdbsmv) fc_ren_sdbsmv	
	, MAX(s3.fc_ren_sfbsmv)	fc_ren_sfbsmv
	, MAX(s3.fc_ren_sdbsfm)	fc_ren_sdbsfm
	, MAX(s3.fc_ren_interes) fc_ren_interes
	, MAX(s3.fc_ren_comfin) fc_ren_comfin
	, MAX(s3.fc_ren_comnofin) fc_ren_comnofin	
	, MAX(s3.fc_ren_rof) fc_ren_rof	
	, MAX(s3.fc_ren_tti) fc_ren_tti	
	, MAX(s3.fc_ren_orex) fc_ren_orex
	, MAX(attr.cod_ren_pers_ods) cod_ren_pers_ods
	, MAX(attr.dt_ren_altacontrato) dt_ren_altacontrato
	, MAX(attr.dt_ren_vencontrato) dt_ren_vencontrato
	, MAX(attr.dt_ren_reestruccontrato) dt_ren_reestruccontrato
	, MAX(attr.cod_ren_entidad_espana) cod_ren_entidad_espana
	, MAX(attr.cod_ren_centro_cont) cod_ren_centro_cont
	, MAX(attr.cod_ren_gestor_prod) cod_ren_gestor_prod
	, MAX(attr.cod_ren_gestor) cod_ren_gestor
	, MAX(attr.cod_ren_vincula) cod_ren_vincula
	, MAX(attr.cod_ren_divisa_mis) cod_ren_divisa_mis
	, MAX(attr.ds_per_nombre_apellido) ds_per_nombre_apellido
	, MAX(attr.cod_per_tipopersona) cod_per_tipopersona
	, MAX(attr.cod_ren_origen_inf) cod_ren_origen_inf
	, MAX(attr.cod_ren_internegocio) cod_ren_internegocio
	, MAX(attr.cod_ren_segmento_gest) cod_ren_segmento_gest
	, MAX(s3.perimetro) perimetro
	, s3.partition_date 
FROM bi_corp_staging.rorac_contratos_s3 s3
LEFT JOIN attr_result attr 
	ON s3.cod_ren_contenido	= attr.cod_ren_contenido
	AND s3.cod_ren_contrato	= attr.cod_ren_contrato
	AND s3.cod_ren_area_negocio = attr.cod_ren_area_negocio	
	AND s3.cod_ren_producto_gest = attr.cod_ren_producto_gest
WHERE s3.partition_date = '2020-12-31' 
GROUP BY s3.cod_ren_contenido	
	, s3.cod_ren_contrato	
	, s3.cod_ren_area_negocio 
	, s3.partition_date ;


DROP TABLE IF EXISTS contratos_s5 ;
CREATE TEMPORARY TABLE contratos_s5 AS 
WITH generic AS (

	SELECT DISTINCT gr.idf_cto_ods
		, gr.idf_pers_ods
		, gr.cod_contenido
		, gr.cod_producto 
		, gr.cod_subprodu
	FROM bi_corp_staging.rio157_ms0_dt_dwh_generic_result gr
	WHERE gr.partition_date = '2020-12-31'
	
)

SELECT s4.cod_ren_contenido	
	, s4.cod_ren_contrato	
	, s4.cod_ren_area_negocio	
	, MAX(s4.cod_ren_producto_gest)	cod_ren_producto_gest
	, MAX(s4.fc_ren_margenmes) fc_ren_margenmes
	, MAX(s4.fc_ren_sdbsmv) fc_ren_sdbsmv	
	, MAX(s4.fc_ren_sfbsmv)	fc_ren_sfbsmv
	, MAX(s4.fc_ren_sdbsfm)	fc_ren_sdbsfm
	, MAX(s4.fc_ren_interes) fc_ren_interes
	, MAX(s4.fc_ren_comfin) fc_ren_comfin
	, MAX(s4.fc_ren_comnofin) fc_ren_comnofin	
	, MAX(s4.fc_ren_rof) fc_ren_rof	
	, MAX(s4.fc_ren_tti) fc_ren_tti	
	, MAX(s4.fc_ren_orex) fc_ren_orex
	, MAX(s4.cod_ren_pers_ods) cod_ren_pers_ods
	, MAX(s4.dt_ren_altacontrato) dt_ren_altacontrato
	, MAX(s4.dt_ren_vencontrato) dt_ren_vencontrato
	, MAX(s4.dt_ren_reestruccontrato) dt_ren_reestruccontrato
	, MAX(s4.cod_ren_entidad_espana) cod_ren_entidad_espana
	, MAX(s4.cod_ren_centro_cont) cod_ren_centro_cont
	, MAX(s4.cod_ren_gestor_prod) cod_ren_gestor_prod
	, MAX(s4.cod_ren_gestor) cod_ren_gestor
	, MAX(s4.cod_ren_vincula) cod_ren_vincula
	, MAX(s4.cod_ren_divisa_mis) cod_ren_divisa_mis
	, MAX(s4.ds_per_nombre_apellido) ds_per_nombre_apellido
	, MAX(s4.cod_per_tipopersona) cod_per_tipopersona
	, MAX(s4.cod_ren_origen_inf) cod_ren_origen_inf
	, MAX(s4.cod_ren_internegocio) cod_ren_internegocio
	, MAX(s4.cod_ren_segmento_gest) cod_ren_segmento_gest 
	, MAX(gr.cod_producto) cod_producto
	, MAX(gr.cod_subprodu) cod_subprodu
	, ROW_NUMBER() OVER(PARTITION BY SUBSTRING(s4.cod_ren_contrato, 1, 26),s4.cod_ren_contenido  ORDER BY 4) int_orden
	, MAX(s4.perimetro) perimetro
	, s4.partition_date
FROM bi_corp_staging.rorac_contratos_s4 s4 
LEFT JOIN generic gr 
	ON s4.cod_ren_contrato = TRIM(gr.idf_cto_ods) 
	AND s4.cod_ren_pers_ods = TRIM(gr.idf_pers_ods) 
	AND s4.cod_ren_contenido = TRIM(gr.cod_contenido) 
GROUP BY s4.cod_ren_contenido	
	, s4.cod_ren_contrato	
	, s4.cod_ren_area_negocio 
	, s4.partition_date ; 

---------------------------------------------------------

DROP TABLE IF EXISTS contratos_s6 ;
CREATE TEMPORARY TABLE contratos_s6 AS
WITH producto_corp AS (

	SELECT TRIM(cod_valor) cod_producto_gest
		, TRIM(cod_valor_padre) cod_producto_mis
		, TRIM(num_nivel) num_nivel
	FROM bi_corp_staging.rio157_ms0_dm_dwh_composicion_jerqs 
	WHERE partition_date = '2020-12-15'
		AND cod_jerarquia = 'JBP01'
		AND cod_dimension = 'PR01'

)

SELECT s6.*
	, adn2.cod_ren_area_negocio_niv_2 cod_ren_division
	, pm.cod_producto_mis cod_ren_producto
	, adn.cod_ren_primercorporativo  cod_ren_area_negociocorp 
FROM contratos_s5 s6  
LEFT JOIN bi_corp_common.dim_ren_areanegocioctr adn2 
	ON s6.cod_ren_area_negocio = adn2.cod_ren_area_negocio_niv_9 
LEFT JOIN producto_corp pm 
	ON s6.cod_ren_producto_gest = PM.cod_producto_gest
LEFT JOIN bi_corp_common.dim_ren_jeareanegocioctr adn 
	ON s6.cod_ren_area_negocio = adn.cod_ren_areanegociohijo ;


DROP TABLE IF EXISTS contratos_s7 ;
CREATE TEMPORARY TABLE contratos_s7 AS
SELECT s6.cod_ren_contenido
		, s6.cod_ren_contrato
		, s6.cod_ren_area_negocio
		, s6.cod_ren_producto_gest
		, s6.fc_ren_margenmes
		, s6.fc_ren_sdbsmv
		, s6.fc_ren_sfbsmv
		, s6.fc_ren_sdbsfm
		, s6.fc_ren_interes
		, s6.fc_ren_comfin
		, s6.fc_ren_comnofin
		, s6.fc_ren_rof
		, s6.fc_ren_tti
		, s6.fc_ren_orex
		, s6.cod_ren_pers_ods
		, s6.dt_ren_altacontrato
		, s6.dt_ren_vencontrato
		, s6.dt_ren_reestruccontrato
		, s6.cod_ren_entidad_espana
		, s6.cod_ren_centro_cont
		, s6.cod_ren_gestor_prod
		, s6.cod_ren_gestor
		, s6.cod_ren_vincula
		, s6.cod_ren_divisa_mis
		, s6.ds_per_nombre_apellido
		, s6.cod_per_tipopersona
		, s6.cod_ren_origen_inf
		, s6.cod_ren_internegocio
		, s6.cod_ren_segmento_gest
		, s6.cod_producto
		, s6.cod_subprodu
		, s6.int_orden
		, s6.partition_date
		, s6.cod_ren_division
		, s6.cod_ren_producto
		, s6.cod_ren_area_negociocorp
		, IF(SUBSTRING(s6.cod_ren_producto_gest, 1, 2) = 'C+', 'A'
			, IF(SUBSTRING(s6.cod_ren_producto_gest, 1, 2) = 'C-', 'P', s6.perimetro)) perimetro 
	FROM contratos_s6 s6 ;



DROP TABLE IF EXISTS contratos_s7b ;
CREATE TEMPORARY TABLE contratos_s7b AS
SELECT s6.cod_ren_contenido
		, s6.cod_ren_contrato
		, s6.cod_ren_area_negocio
		, MAX(s6.cod_ren_producto_gest) cod_ren_producto_gest
		, MAX(s6.fc_ren_margenmes) fc_ren_margenmes
		, MAX(s6.fc_ren_sdbsmv) fc_ren_sdbsmv
		, MAX(s6.fc_ren_sfbsmv) fc_ren_sfbsmv
		, MAX(s6.fc_ren_sdbsfm) fc_ren_sdbsfm
		, MAX(s6.fc_ren_interes) fc_ren_interes
		, MAX(s6.fc_ren_comfin) fc_ren_comfin
		, MAX(s6.fc_ren_comnofin) fc_ren_comnofin
		, MAX(s6.fc_ren_rof) fc_ren_rof
		, MAX(s6.fc_ren_tti) fc_ren_tti
		, MAX(s6.fc_ren_orex) fc_ren_orex
		, MAX(s6.cod_ren_pers_ods) cod_ren_pers_ods
		, MAX(s6.dt_ren_altacontrato) dt_ren_altacontrato
		, MAX(s6.dt_ren_vencontrato) dt_ren_vencontrato
		, MAX(s6.dt_ren_reestruccontrato) dt_ren_reestruccontrato
		, MAX(s6.cod_ren_entidad_espana) cod_ren_entidad_espana
		, MAX(s6.cod_ren_centro_cont) cod_ren_centro_cont
		, MAX(s6.cod_ren_gestor_prod) cod_ren_gestor_prod
		, MAX(s6.cod_ren_gestor) cod_ren_gestor
		, MAX(s6.cod_ren_vincula) cod_ren_vincula
		, MAX(s6.cod_ren_divisa_mis) cod_ren_divisa_mis
		, MAX(s6.ds_per_nombre_apellido) ds_per_nombre_apellido
		, MAX(s6.cod_per_tipopersona) cod_per_tipopersona
		, MAX(s6.cod_ren_origen_inf) cod_ren_origen_inf
		, MAX(s6.cod_ren_internegocio) cod_ren_internegocio
		, MAX(s6.cod_ren_segmento_gest) cod_ren_segmento_gest
		, MAX(s6.cod_producto) cod_producto
		, MAX(s6.cod_subprodu) cod_subprodu
		, MAX(s6.int_orden) int_orden
		, MAX(s6.partition_date) partition_date
		, MAX(s6.cod_ren_division) cod_ren_division
		, MAX(s6.cod_ren_producto) cod_ren_producto
		, MAX(s6.cod_ren_area_negociocorp) cod_ren_area_negociocorp
		, MAX(IF(s6.perimetro IS NULL, COALESCE(s7.perimetro, 'A'), s6.perimetro)) perimetro 
	FROM contratos_s7 s6 
	LEFT JOIN contratos_s7 s7
		ON s6.cod_ren_contrato = s7.cod_ren_contrato
		AND s7.perimetro IS NOT NULL 
		AND s6.perimetro IS NULL 
	GROUP BY s6.cod_ren_contenido
		, s6.cod_ren_contrato
		, s6.cod_ren_area_negocio ;


DROP TABLE IF EXISTS contratos_s8 ;
CREATE TEMPORARY TABLE contratos_s8 AS

WITH id_bdr AS (

	SELECT DISTINCT MK.native_key cod_ren_contrato_rorac
		, BDR.native_key id_cto_bdr
	FROM bi_corp_common.rosetta_nkey_hist MK
		JOIN bi_corp_common.rosetta_nkey_hist BDR ON 
			MK.master_key = BDR.master_key 
			AND BDR.partition_date = '2020-12-31'
			AND BDR.domain_code = '00004'
	WHERE MK.partition_date = '2020-12-31'
		AND MK.domain_code = '00005'

)

, contratos_bdr AS (

	SELECT CONCAT(s5.cod_ren_contenido, s5.cod_ren_contrato) cod_ren_contrato_rorac
		, bdr.id_cto_bdr
		, s5.*
	FROM contratos_s7b s5 
	LEFT JOIN id_bdr bdr 
	ON CONCAT(s5.cod_ren_contenido, s5.cod_ren_contrato) = bdr.cod_ren_contrato_rorac
	AND s5.int_orden = 1 
	AND s5.perimetro = 'A'
)

SELECT * FROM contratos_bdr ;



DROP TABLE IF EXISTS contratos_s9 ;
CREATE TEMPORARY TABLE contratos_s9 AS
WITH saldo_ifrs AS (

	SELECT CONCAT(t_cod_entidad, t_cod_centro, t_cod_producto, lpad(t_cod_subprodu_altair,4,'0'), t_num_cuenta) cod_ren_contrato_short
		, t_idf_cto_ifrs
		, t_cod_entidad
		, t_cod_centro
		, t_num_cuenta
		, t_cod_producto
		, lpad(t_cod_subprodu_altair,4,'0') t_cod_subprodu_altair
		, t_cod_divisa
		, SUM(s_provision_amount) s_provision_amount
	FROM santander_business_risk_ifrs9.ifrs9_tablon
	WHERE periodo = '20201230'
		AND t_cod_divisa = 'ARS'
		AND t_cod_entidad IS NOT NULL
		AND t_cod_centro IS NOT NULL
		AND t_cod_producto IS NOT NULL
		AND t_cod_subprodu_altair IS NOT NULL
		AND t_cod_divisa IS NOT NULL
	GROUP BY t_idf_cto_ifrs
		, t_cod_entidad
		, t_cod_centro
		, t_num_cuenta
		, t_cod_producto
		, lpad(t_cod_subprodu_altair,4,'0') 
		, t_cod_divisa

)

SELECT s6.*
	, ifrs.s_provision_amount fc_ifrs_provision
FROM contratos_s8 s6
LEFT JOIN saldo_ifrs ifrs ON 
	SUBSTRING(s6.cod_ren_contrato, 1, 26) = ifrs.cod_ren_contrato_short 
	AND s6.int_orden = 1
	AND s6.perimetro = 'A'
	AND s6.cod_ren_contrato != '-99100' ;

