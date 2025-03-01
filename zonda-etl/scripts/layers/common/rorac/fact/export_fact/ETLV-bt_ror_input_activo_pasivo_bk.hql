SET mapred.job.queue.name=root.dataeng;

DROP TABLE IF EXISTS cto_contenido ;
CREATE TEMPORARY TABLE cto_contenido AS
WITH 

cto_act_pas AS (

	SELECT DISTINCT RE.cod_ren_contrato 
			, RE.cod_ren_contenido 
			, RE.cod_ren_contrato_rorac 
			, IF(RE.cod_ren_contenido IN ('PLZ','CTA'), 'P', IF(RE.cod_ren_contenido IN ('PRE','CRE','ARF','CCO','ROF'), 'A', cast(NULL as string))) perimetro
			, RE.cod_ren_producto_niv_3 
			, RE.cod_ren_area_negocio 
			, RE.cod_ren_producto_gest 
	FROM bi_corp_common.bt_ren_result RE
	WHERE RE.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_to', dag_id='LOAD_CMN_RORAC_inputs') }}'
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
				, IF(SUBSTRING(RE.cod_ren_producto_gest, 1, 1) = 'O', 'P', cast(NULL as string) ))))) perimetro
			, RE.cod_ren_producto_gest
			, RE.cod_ren_area_negocio 
	FROM bi_corp_common.bt_ren_result RE
	WHERE RE.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_to', dag_id='LOAD_CMN_RORAC_inputs') }}'
	AND RE.cod_ren_contenido IN ('COM','CTB','CCL') 
	AND RE.cod_ren_contrato = '-99100'
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


DROP TABLE IF EXISTS bi_corp_staging.zror_perimetro_activo ;
CREATE TABLE bi_corp_staging.zror_perimetro_activo STORED AS PARQUET
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
		, IF(SUBSTRING(RE.cod_ren_contrato,11,4) = '', cast(NULL as string), SUBSTRING(RE.cod_ren_contrato,11,4)) cod_ren_subprodu
		, RE.cod_ren_gestor_prod 
		, IF(RE.cod_ren_gestor = 'null', cast(NULL as string), RE.cod_ren_gestor) cod_ren_gestor
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
	WHERE RE.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_to', dag_id='LOAD_CMN_RORAC_inputs') }}'
		AND RE.cod_ren_contrato = CA.cod_ren_contrato
		AND RE.cod_ren_contenido = CA.cod_ren_contenido
		AND RE.cod_ren_contrato_rorac = CA.cod_ren_contrato_rorac
		AND RE.cod_ren_contenido IN ('PRE','CRE','ARF','CCO','COM','CTB','ROF','CCL')
		AND RE.cod_ren_area_negocio = CA.cod_ren_area_negocio
		AND RE.cod_ren_producto_gest = CA.cod_ren_producto_gest
		AND RE.cod_ren_contrato != '-99100' ;
		
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
		, IF(SUBSTRING(RE.cod_ren_contrato,11,4) = '', cast(NULL as string), SUBSTRING(RE.cod_ren_contrato,11,4)) cod_ren_subprodu
		, RE.cod_ren_gestor_prod 
		, IF(RE.cod_ren_gestor = 'null', cast(NULL as string), RE.cod_ren_gestor) cod_ren_gestor
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
	WHERE RE.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_to', dag_id='LOAD_CMN_RORAC_inputs') }}'
		AND RE.cod_ren_contrato = PA.cod_ren_contrato
		AND RE.cod_ren_contenido = PA.cod_ren_contenido
		AND RE.cod_ren_producto_gest = PA.cod_ren_producto_gest
		AND RE.cod_ren_contenido IN ('COM','CTB','CCL')
		AND RE.cod_ren_area_negocio = PA.cod_ren_area_negocio
		AND RE.cod_ren_producto_gest = PA.cod_ren_producto_gest
		AND RE.cod_ren_contrato = '-99100' ;


DROP TABLE IF EXISTS bi_corp_staging.zror_perimetro_pasivo ;
CREATE TABLE bi_corp_staging.zror_perimetro_pasivo AS 
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
		, IF(SUBSTRING(RE.cod_ren_contrato,11,4) = '', cast(NULL as string), SUBSTRING(RE.cod_ren_contrato,11,4)) cod_ren_subprodu
		, RE.cod_ren_gestor_prod 
		, IF(RE.cod_ren_gestor = 'null', cast(NULL as string), RE.cod_ren_gestor) cod_ren_gestor
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
	WHERE RE.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_to', dag_id='LOAD_CMN_RORAC_inputs') }}'
		AND RE.cod_ren_contrato = CP.cod_ren_contrato
		AND RE.cod_ren_contenido = CP.cod_ren_contenido
		AND RE.cod_ren_contrato_rorac = CP.cod_ren_contrato_rorac
		AND RE.cod_ren_contenido IN ('PLZ','CTA','COM','CTB','CCL')
		AND RE.cod_ren_contrato != '-99100' 
		AND RE.cod_ren_area_negocio = CP.cod_ren_area_negocio 
		AND RE.cod_ren_producto_gest = CP.cod_ren_producto_gest ;
	
	INSERT INTO bi_corp_staging.zror_perimetro_pasivo
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
		, IF(SUBSTRING(RE.cod_ren_contrato,11,4) = '', cast(NULL as string), SUBSTRING(RE.cod_ren_contrato,11,4)) cod_ren_subprodu
		, RE.cod_ren_gestor_prod 
		, IF(RE.cod_ren_gestor = 'null', cast(NULL as string), RE.cod_ren_gestor) cod_ren_gestor
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
	WHERE RE.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_to', dag_id='LOAD_CMN_RORAC_inputs') }}'
		AND RE.cod_ren_contrato = PP.cod_ren_contrato
		AND RE.cod_ren_contenido = PP.cod_ren_contenido
		AND RE.cod_ren_producto_gest = PP.cod_ren_producto_gest
		AND RE.cod_ren_contenido IN ('COM','CTB','CCL')
		AND RE.cod_ren_contrato = '-99100' 
		AND RE.cod_ren_producto_gest = PP.cod_ren_producto_gest
		AND RE.cod_ren_area_negocio = PP.cod_ren_area_negocio ;


DROP TABLE IF EXISTS bi_corp_staging.zror_producto_mis ;
CREATE TEMPORARY TABLE  bi_corp_staging.zror_producto_mis AS
	SELECT TRIM(cod_valor) cod_producto_gest
		, TRIM(cod_valor_padre) cod_producto_mis
		, TRIM(num_nivel) num_nivel
	FROM bi_corp_staging.rio157_ms0_dm_dwh_composicion_jerqs 
	WHERE partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_rio157_ms0_dm_dwh_composicion_jerqs', dag_id='LOAD_CMN_RORAC_inputs') }}'
		AND cod_jerarquia = 'JBP01'
		AND cod_dimension = 'PR01' ;


DROP TABLE IF EXISTS bi_corp_staging.zror_tablon_ifrs9 ;
CREATE TEMPORARY TABLE bi_corp_staging.zror_tablon_ifrs9 AS  
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
	WHERE RI.periodo = '{{ ti.xcom_pull(task_ids='InputConfig', key='last_working_day', dag_id='LOAD_CMN_RORAC_inputs') }}'
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


DROP TABLE IF EXISTS bi_corp_staging.zror_rosetta_key ;
CREATE TEMPORARY TABLE bi_corp_staging.zror_rosetta_key AS
	SELECT DISTINCT MK.native_key AS cod_ren_contrato_rorac
		, BDR.native_key AS id_cto_bdr
	FROM bi_corp_common.rosetta_nkey_hist MK
		JOIN bi_corp_common.rosetta_nkey_hist BDR ON 
			MK.master_key = BDR.master_key 
			AND BDR.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_to', dag_id='LOAD_CMN_RORAC_inputs') }}'
			AND BDR.domain_code = '00004'
	WHERE MK.partition_date = '{{ ti.xcom_pull(task_ids='InputConfig', key='month_to', dag_id='LOAD_CMN_RORAC_inputs') }}'
		AND MK.domain_code = '00005';


	DROP TABLE IF EXISTS bi_corp_staging.zror_activo_01 ;
	CREATE TEMPORARY TABLE bi_corp_staging.zror_activo_01
	AS SELECT RE.*
		, adn2.cod_ren_area_negocio_niv_2 cod_ren_division
		, pm.cod_producto_mis cod_ren_producto
		, adn.cod_ren_primercorporativo  cod_ren_area_negociocorp 
	FROM bi_corp_staging.zror_perimetro_activo RE  

	LEFT JOIN bi_corp_common.dim_ren_areanegocioctr adn2 ON 
		RE.cod_ren_area_negocio = adn2.cod_ren_area_negocio_niv_9 

	LEFT JOIN bi_corp_staging.zror_producto_mis pm ON 
		RE.cod_productogest = PM.cod_producto_gest

	LEFT JOIN bi_corp_common.dim_ren_jeareanegocioctr adn ON 
		RE.cod_ren_area_negocio = adn.ds_ren_areanegociohijo ;


	DROP TABLE IF EXISTS bi_corp_staging.zror_activo_02 ;
	CREATE TABLE bi_corp_staging.zror_activo_02 STORED AS PARQUET
	AS SELECT RE.*
		, RK.id_cto_bdr		
		, ROW_NUMBER() OVER(PARTITION BY SUBSTRING(RE.cod_ren_contrato, 1, 26)) int_ren_ordercontrato
	FROM bi_corp_staging.zror_activo_01 RE

	LEFT JOIN bi_corp_staging.zror_rosetta_key RK ON 
		RE.cod_ren_contrato_rorac = RK.cod_ren_contrato_rorac 
	WHERE RE.cod_ren_contrato != '-99100' ;

	INSERT INTO bi_corp_staging.zror_activo_02
	SELECT DISTINCT RE.*
		, cast(NULL as string) id_cto_bdr
		, cast(NULL as int) int_ren_ordercontrato
	FROM bi_corp_staging.zror_activo_01 RE
	WHERE RE.cod_ren_contrato = '-99100' ;

	
	DROP TABLE IF EXISTS bi_corp_staging.zror_activo ;
	CREATE TABLE bi_corp_staging.zror_activo STORED AS PARQUET
	AS SELECT RE.*
		, RI.s_provision_amount fc_ifrs_provision
	FROM bi_corp_staging.zror_activo_02 RE

	LEFT JOIN bi_corp_staging.zror_tablon_ifrs9 RI ON 
		(SUBSTRING(RE.cod_ren_contrato,1,26) = RI.cod_ren_contrato_short
		and RE.int_ren_ordercontrato = 1) 
	WHERE RE.cod_ren_contrato != '-99100' ;

	INSERT INTO bi_corp_staging.zror_activo
	SELECT DISTINCT RE.*
		, cast(NULL as decimal(20, 4)) fc_ifrs_provision
	FROM bi_corp_staging.zror_activo_02 RE
	WHERE RE.cod_ren_contrato = '-99100' ;


	DROP TABLE IF EXISTS bi_corp_staging.zror_pasivo ;
	CREATE TABLE bi_corp_staging.zror_pasivo STORED AS PARQUET
	AS SELECT RE.*
		, adn2.cod_ren_area_negocio_niv_2 cod_ren_division
		, pm.cod_producto_mis cod_ren_producto
		, adn.cod_ren_primercorporativo  cod_ren_area_negociocorp 
		, 0 id_cto_bdr
		, 0 int_ren_ordercontrato
		, 0 fc_ifrs_provision
	FROM bi_corp_staging.zror_perimetro_pasivo RE	 

	LEFT JOIN bi_corp_common.dim_ren_areanegocioctr adn2 ON 
		RE.cod_ren_area_negocio = adn2.cod_ren_area_negocio_niv_9 

	LEFT JOIN bi_corp_staging.zror_producto_mis pm ON 
		RE.cod_productogest = PM.cod_producto_gest

	LEFT JOIN bi_corp_common.dim_ren_jeareanegocioctr adn ON 
		RE.cod_ren_area_negocio = adn.ds_ren_areanegociohijo ;


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
		partition_date
	FROM bi_corp_staging.zror_activo 
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
	FROM bi_corp_staging.zror_pasivo
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