




---------------------------------------------------TEMP_RESULT_PASIVO
-- SELECT COUNT(1) FROM perimetro_pasivo ; -- 135.158.705
SET mapred.job.queue.name=root.dataeng;
--------------------------------------------------------
CREATE TEMPORARY TABLE IF NOT EXISTS perimetro_pasivo AS 
SELECT RE.cod_ren_contrato
	, RE.cod_ren_cliente_rorac
	, RE.cod_ren_contrato_rorac
	, RE.cod_ren_pers_ods
	, RE.cod_ren_contenido
	, RE.cod_ren_area_negocio
	, RE.cod_ren_producto_niv_3
	, date_format(RE.dt_ren_altacontrato, 'dd-MM-yyyy') dt_ren_fec_altacto 
	, date_format(RE.dt_ren_vencontrato, 'dd-MM-yyyy') dt_ren_fec_ven 
	, RE.cod_ren_entidad_espana 
	, RE.cod_ren_centro_cont cod_ren_centrocont
	, IF(SUBSTRING(RE.cod_ren_contrato,11,4) = '', NULL, SUBSTRING(RE.cod_ren_contrato,11,4)) cod_ren_subprodu
	, RE.cod_ren_gestor_prod cod_ren_gestorprod
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
	, IF(RE.dt_ren_reestruccontrato IS NULL, '01-01-9999', date_format(RE.dt_ren_reestruccontrato, 'dd-MM-yyyy')) dt_ren_fec_reestruc
	, date_format(RE.partition_date, 'dd-MM-yyyy') dt_ren_fec_extrdatos
	, ROW_NUMBER() OVER(PARTITION BY SUBSTRING(RE.cod_ren_contrato, 1, 26)) int_ren_ordercontrato
	----------------------------
	, RE.fc_ren_resultado_total_ml
	, RE.fc_ren_saldo_medio_ml
	, RE.fc_ren_sdo_med_cap_ml
	, RE.fc_ren_sdo_med_int_ml
	, RE.fc_ren_sdo_med_reajuste_ml
	, RE.fc_ren_sdo_med_insolv_ml
	, RE.fc_ren_ing_per_ml
	, RE.fc_ren_ing_cap_ml
	, RE.fc_ren_resultado_total_real_ml
	---------------------------
	, RE.partition_date 
FROM bi_corp_common.bt_ren_result RE
WHERE RE.partition_date = '2020-10-31'
	AND RE.cod_ren_contenido IN ('PLZ','CTA') ; --'COM','CTB','ROF','CCL' --'COM','CTB','ROF','CCL'


('CTB','ROF','CCL')

-------------------------------------------------------------

-- ACTIVO
--------------------------------------------------------------
-- DROP TABLE activo
-- SELECT COUNT(1) FROM activo ; -- 258.286.174
-- SELECT * FROM activo LIMIT 20 ;
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
CREATE TEMPORARY TABLE IF NOT EXISTS activo AS
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
	RE.dt_ren_fec_altacto ,
	RE.dt_ren_fec_ven ,
	RE.cod_ren_entidad_espana ,
	RE.cod_ren_centrocont ,
	RE.cod_ren_subprodu ,
	NULL cod_ren_zona ,
	NULL cod_ren_territorial ,
	RE.cod_ren_gestorprod ,
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
	RE.dt_ren_fec_reestruc ,
	date_format(RE.partition_date, 'dd-MM-yyyy') dt_ren_fec_extrdatos ,
	IF(RE.cod_ren_contenido = 'CCL', 2, 0) flag_ren_neteo ,
	CASE WHEN RE.cod_ren_producto_niv_3 = 'BG1' AND RE.cod_ren_cta_resultados_niv_8 = '1054' THEN SUM(RE.fc_ren_resultado_total_real_ml) ELSE 0 END fc_ren_orex ,
	SUM(RI.s_provision_amount) fc_ifrs_provision ,
	NULL fc_ren_costemensualcto ,
    IF(RE.cod_ren_contenido NOT IN ('CCL','CTB'), 0, IF(RE.cod_ren_pers_ods != '-99100', 1, 3)) cod_nivel_operacion ,
    RE.partition_date
-------------------------------------------------------RESULT
FROM perimetro_activo RE
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
GROUP BY IF(RE.cod_ren_contenido IN ('CCL','CTB'), RE.cod_ren_cliente_rorac, RE.cod_ren_contrato_rorac) ,
	RE.cod_ren_contrato ,
	RE.cod_ren_area_negocio ,
	adn2.cod_ren_area_negocio_niv_2 ,
	pm.cod_producto_mis ,
	RE.cod_ren_pers_ods ,
	RE.dt_ren_fec_altacto ,
	RE.dt_ren_fec_ven ,
	RE.cod_ren_entidad_espana ,
	RE.cod_ren_centrocont ,
	RE.cod_ren_subprodu ,
	RE.cod_ren_gestorprod ,
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
	RE.dt_ren_fec_reestruc ,
	date_format(RE.partition_date, 'dd-MM-yyyy') ,
	IF(RE.cod_ren_contenido = 'CCL', 2, 0) ,
	IF(RE.cod_ren_contenido NOT IN ('CCL','CTB'), 0, IF(RE.cod_ren_pers_ods != '-99100', 1, 3)) ,
    RE.partition_date ,
    RE.cod_ren_producto_niv_3 ,
    RE.cod_ren_cta_resultados_niv_9 ,
    RE.cod_ren_cta_resultados_niv_8 ;
--------------------------------------------------------------

-- INSERT TABLE
INSERT OVERWRITE TABLE bi_corp_common.bt_ror_input_activo
PARTITION(partition_date) 
SELECT RE.cod_ren_unidad ,
	RE.dt_ren_fec_data ,
	RE.cod_ren_finalidad_datos ,
	RE.cod_ren_contrato_rorac ,
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
	IF(RE.dt_ren_fec_ven IS NULL, '01-01-9999', RE.dt_ren_vencontrat) ,
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
FROM activo RE
--LEFT JOIN tablon_ifrs9 RI ON 
--	SUBSTRING(RE.cod_ren_contrato,1,26) = RI.cod_ren_contrato_short
--	AND RE.order_contrato = 1 
--LEFT JOIN rosetta_key RK ON 
--	RE.cod_ren_contrato_rorac = RK.cod_ren_contrato_rorac 
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
   
   
 

-- PASIVO
--------------------------------------------------------------
SET hive.execution.engine=spark;
SET spark.yarn.queue=root.dataeng;
SET mapred.job.queue.name=root.dataeng;
SET hive.exec.dynamic.partition.mode=nonstrict;
--------------------------------------------------------------
CREATE TEMPORARY TABLE IF NOT EXISTS pasivo AS
SELECT 'ARG' cod_ren_unidad ,
	date_format(RE.partition_date, 'yyyyMM') dt_ren_data ,
	1 cod_ren_finalidaddatos ,
	IF(RE.cod_ren_contenido IN ('CCL','CTB'), CONCAT(RE.cod_ren_contenido,RE.cod_ren_pers_ods), CONCAT(RE.cod_ren_contenido,RE.cod_ren_contrato)) cod_ren_contrato_rorac ,
	RE.cod_ren_contrato ,
	RE.cod_ren_area_negocio cod_ren_areanegocio ,
	'ARS' cod_ren_divisa ,
	adn2.cod_ren_area_negocio_niv_2 cod_ren_division ,
	pm.cod_producto_mis cod_ren_producto ,
	RE.cod_ren_pers_ods ,
	CASE WHEN RE.cod_ren_producto_niv_3 = 'BG2' THEN SUM(RE.fc_ren_resultado_total_ml) ELSE 0 END fc_ren_margenmes ,
	CASE WHEN RE.cod_ren_producto_niv_3 = 'BG2' THEN SUM(RE.fc_ren_saldo_medio_ml) ELSE 0 END fc_ren_sdbsmv ,
	CASE WHEN RE.cod_ren_producto_niv_3 = 'BG3' THEN (SUM(RE.fc_ren_sdo_med_cap_ml)
										+ SUM(RE.fc_ren_sdo_med_int_ml)
										+ SUM(RE.fc_ren_sdo_med_reajuste_ml)
										+ SUM(RE.fc_ren_sdo_med_insolv_ml)) ELSE 0 END fc_ren_sfbsmv ,
	date_format(RE.dt_ren_altacontrato, 'dd-MM-yyyy') dt_ren_fec_altacto ,
	date_format(RE.dt_ren_vencontrato, 'dd-MM-yyyy') dt_ren_fec_ven ,
	RE.cod_ren_entidad_espana ,
	RE.cod_ren_centro_cont cod_ren_centrocont ,
	IF(SUBSTRING(RE.cod_ren_contrato,11,4) = '', NULL, SUBSTRING(RE.cod_ren_contrato,11,4)) cod_ren_subprodu ,
	NULL cod_ren_zona ,
	NULL cod_ren_territorial ,
	RE.cod_ren_gestor_prod cod_ren_gestorprod ,
	IF(RE.cod_ren_gestor = 'null', NULL, RE.cod_ren_gestor) cod_ren_gestor ,
	RK.id_cto_bdr id_cto_bdr ,
	CASE WHEN RE.cod_ren_producto_niv_3 = 'BG2' THEN (SUM(RE.fc_ren_ing_per_ml)
										+ SUM(RE.fc_ren_ing_cap_ml)
										+ SUM(RE.fc_ren_sdo_med_cap_ml)
										+ SUM(RE.fc_ren_sdo_med_int_ml)
										+ SUM(RE.fc_ren_sdo_med_reajuste_ml)
										+ SUM(RE.fc_ren_sdo_med_insolv_ml)) ELSE 0 END fc_ren_sdbsfm ,
	CASE WHEN RE.cod_ren_producto_niv_3 = 'BG2' AND RE.cod_ren_cta_resultados_niv_9 = '1031' THEN SUM(RE.fc_ren_resultado_total_real_ml) ELSE 0 END fc_ren_interes ,
	CASE WHEN RE.cod_ren_producto_niv_3 = 'BG2' AND RE.cod_ren_cta_resultados_niv_9 = '1032' THEN SUM(RE.fc_ren_resultado_total_real_ml) ELSE 0 END fc_ren_comfin ,
	CASE WHEN RE.cod_ren_producto_niv_3 = 'BG2' AND RE.cod_ren_cta_resultados_niv_8 = '1051' THEN SUM(RE.fc_ren_resultado_total_real_ml) ELSE 0 END fc_ren_comnofin ,
	IF(RE.cod_ren_vincula = 'null', 0, RE.cod_ren_vincula) cod_ren_vincula ,
	CASE WHEN RE.cod_ren_producto_niv_3 = 'BG2' AND RE.cod_ren_cta_resultados_niv_8 = '1052' THEN SUM(RE.fc_ren_resultado_total_real_ml) ELSE 0 END fc_ren_rof ,
	SUM(fc_ren_resultado_total_ml) fc_ren_tti ,
	NULL cod_ren_mtm ,
	NULL cod_ren_nocional ,
	RE.cod_ren_divisa cod_ren_divisa_mis ,
	NULL flag_ren_moralocal ,
	NULL flag_ren_carterizadas ,
	RE.ds_per_nombre_apellido ds_ren_nombrecliente ,
	IF(RE.cod_per_tipopersona = 'null', 'F', RE.cod_per_tipopersona) cod_per_tipopersona ,
	NULL cod_ren_nifcif ,
	RE.cod_ren_origen_inf ds_ren_intragrupo ,
	CONCAT(TRIM(RE.cod_ren_tipo_ajuste),TRIM(RE.cod_ren_origen_inf)) cod_ren_internegocio ,
	adn.cod_ren_primercorporativo cod_ren_areanegociocorp ,
	RE.cod_ren_producto_gest cod_productogest ,
	RE.cod_ren_segmento_gest cod_segmentogest ,
	RE.cod_ren_producto_generic cod_producto_operacional ,
	CONCAT(regexp_replace(RE.partition_date, '-', ''), 'v1') ds_ren_ficheromis ,
	IF(RE.dt_ren_reestruccontrato IS NULL, '01-01-9999', date_format(RE.dt_ren_reestruccontrato, 'dd-MM-yyyy')) dt_ren_fec_reestruc ,
	date_format(RE.partition_date, 'dd-MM-yyyy') dt_ren_fec_extrdatos ,
	IF(RE.cod_ren_contenido = 'CCL', 2, 0) flag_ren_neteo ,
	CASE WHEN RE.cod_ren_producto_niv_3 = 'BG2' AND RE.cod_ren_cta_resultados_niv_8 = '1054' THEN SUM(RE.fc_ren_resultado_total_real_ml) ELSE 0 END fc_ren_orex ,
	--SUM(RI.s_provision_amount) fc_ifrs_provision ,
	NULL fc_ren_costemensualcto ,
    IF(RE.cod_ren_contenido NOT IN ('CCL','CTB'), 0, IF(RE.cod_ren_pers_ods != '-99100', 1, 3)) cod_nivel_operacion ,
    ROW_NUMBER() OVER(PARTITION BY SUBSTRING(RE.cod_ren_contrato, 1, 26)) int_ren_ordercontrato
-------------------------------------------------------RESULT
FROM bi_corp_common.bt_ren_result RE
-------------------------------------------------------AREANEGOCIOCTR
LEFT JOIN bi_corp_common.dim_ren_areanegocioctr adn2 ON 
	RE.cod_ren_area_negocio = adn2.cod_ren_area_negocio_niv_9 
-------------------------------------------------------PRODUCTOMIS
LEFT JOIN producto_mis pm ON 
	RE.cod_ren_producto_gest = PM.cod_producto_gest
-------------------------------------------------------AREANEGOCIOCORP
LEFT JOIN bi_corp_common.dim_ren_jeareanegocioctr adn ON 
	RE.cod_ren_area_negocio = adn.cod_ren_areanegociohijo
-------------------------------------------------------ROSETTA
LEFT JOIN rosetta_key RK ON 
	--RE.cod_ren_contrato_rorac = RK.cod_ren_contrato_rorac 
	CONCAT(RE.cod_ren_contenido, RE.cod_ren_contrato) = RK.cod_ren_contrato_rorac
-------------------------------------------------------
GROUP BY date_format(RE.partition_date, 'yyyyMM') --dt_ren_data ,
	, IF(RE.cod_ren_contenido IN ('CCL','CTB'), CONCAT(RE.cod_ren_contenido,RE.cod_ren_pers_ods), CONCAT(RE.cod_ren_contenido,RE.cod_ren_contrato)) --cod_ren_contrato_rorac ,
	, RE.cod_ren_contrato --,
	, RE.cod_ren_area_negocio --cod_ren_areanegocio ,
	, adn2.cod_ren_area_negocio_niv_2 --cod_ren_division ,
	, pm.cod_producto_mis --cod_ren_producto ,
	, RE.cod_ren_pers_ods --,
	, date_format(RE.dt_ren_altacontrato, 'dd-MM-yyyy') --dt_ren_fec_altacto ,
	, date_format(RE.dt_ren_vencontrato, 'dd-MM-yyyy') --dt_ren_fec_ven ,
	, RE.cod_ren_entidad_espana --,
	, RE.cod_ren_centro_cont --cod_ren_centrocont ,
	, IF(SUBSTRING(RE.cod_ren_contrato,11,4) = '', NULL, SUBSTRING(RE.cod_ren_contrato,11,4)) --cod_ren_subprodu ,
	, RE.cod_ren_gestor_prod --cod_ren_gestorprod ,
	, IF(RE.cod_ren_gestor = 'null', NULL, RE.cod_ren_gestor) --cod_ren_gestor ,
	, RK.id_cto_bdr --id_cto_bdr ,
	, IF(RE.cod_ren_vincula = 'null', 0, RE.cod_ren_vincula)
	, RE.cod_ren_divisa --cod_ren_divisa_mis ,
	, RE.ds_per_nombre_apellido --ds_ren_nombrecliente ,
	, IF(RE.cod_per_tipopersona = 'null', 'F', RE.cod_per_tipopersona) --cod_per_tipopersona ,
	, RE.cod_ren_origen_inf --ds_ren_intragrupo ,
	, CONCAT(TRIM(RE.cod_ren_tipo_ajuste),TRIM(RE.cod_ren_origen_inf)) --cod_ren_internegocio ,
	, adn.cod_ren_primercorporativo --cod_ren_areanegociocorp ,
	, RE.cod_ren_producto_gest --cod_productogest ,
	, RE.cod_ren_segmento_gest --cod_segmentogest ,
	, RE.cod_ren_producto_generic --cod_producto_operacional ,
	, CONCAT(regexp_replace(RE.partition_date, '-', ''), 'v1') --ds_ren_ficheromis ,
	, IF(RE.dt_ren_reestruccontrato IS NULL, '01-01-9999', date_format(RE.dt_ren_reestruccontrato, 'dd-MM-yyyy')) --dt_ren_fec_reestruc ,
	, date_format(RE.partition_date, 'dd-MM-yyyy') --dt_ren_fec_extrdatos ,
	, IF(RE.cod_ren_contenido = 'CCL', 2, 0) --flag_ren_neteo ,
	, IF(RE.cod_ren_contenido NOT IN ('CCL','CTB'), 0, IF(RE.cod_ren_pers_ods != '-99100', 1, 3)) --cod_nivel_operacion
    , RE.cod_ren_producto_niv_3
    , RE.cod_ren_cta_resultados_niv_9
    , RE.cod_ren_cta_resultados_niv_8 ;
---------------------------------------



/*
set hive.execution.engine=spark;
set spark.yarn.queue=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

INSERT OVERWRITE TABLE bi_corp_common.dim_ren_jeareanegocioctr
SELECT 
	 TRIM(cod_pais) cod_ren_pais ,
	 TRIM(cod_padre) cod_ren_areanegociopadre ,
	 TRIM(cod_hijo) cod_ren_areanegociohijo ,
	 TRIM(des_hijo) ds_ren_areanegociohijo ,
	 TRIM(cod_jerarquia) cod_ren_areanegociojerarquia ,
	 IF(ind_consolidacion = 'null', NULL, TRIM(ind_consolidacion)) flag_ren_consolidacion ,
	 IF(cod_primer_corporativo = 'null', NULL, TRIM(cod_primer_corporativo)) cod_ren_primercorporativo ,
	 TRIM(userid_umo) cod_ren_userumo ,
	 from_unixtime(unix_timestamp(TRIM(SUBSTRING(timest_umo, 0,19)),'yyyy-MM-dd HH:mm:ss')) ts_ren_timestumo ,
	 TRIM(num_nivel) int_ren_nivel ,
	 TRIM(ind_corporativo) cod_ren_indcorporativo ,
	 TRIM(num_orden) int_ren_orden ,
	partition_date
FROM
	bi_corp_staging.rio157_ms0_dm_je_dwh_area_negocio_ctr 
WHERE partition_date = '2020-10-09'
	AND cod_jerarquia = 'JAN01' ;
	
SELECT * FROM activo
*/

   
CREATE TEMPORARY TABLE IF NOT EXISTS perimetro_cre AS
SELECT RE.*
	, CONCAT(TRIM(RE.cod_ren_tipo_ajuste),TRIM(RE.cod_ren_origen_inf)) cod_ren_internegocio
	, IF(SUBSTRING(RE.cod_ren_contrato,11,4) = '', NULL, SUBSTRING(RE.cod_ren_contrato,11,4)) cod_ren_subprodu
	, adn2.cod_ren_area_negocio_niv_2 cod_ren_division
	, pm.cod_producto_mis cod_ren_producto 
	, adn.cod_ren_primercorporativo cod_ren_areanegociocorp
FROM bi_corp_common.bt_ren_result RE
LEFT JOIN bi_corp_common.dim_ren_areanegocioctr adn2 ON 
	RE.cod_ren_area_negocio = adn2.cod_ren_area_negocio_niv_9 
LEFT JOIN producto_mis pm ON 
	RE.cod_ren_producto_gest = PM.cod_producto_gest
LEFT JOIN bi_corp_common.dim_ren_jeareanegocioctr adn ON 
	RE.cod_ren_area_negocio = adn.cod_ren_areanegociohijo
WHERE RE.partition_date = '2020-10-31' 
	AND RE.cod_ren_contenido = 'PRE' AND RE.cod_ren_cta_resultados_niv_6 != '1073' AND RE.cod_ren_producto_niv_3 IN ('BG1','BG3') ;



---------------------------------------------------------
CREATE TEMPORARY TABLE IF NOT EXISTS perimetro_pre AS 
SELECT RE.*
	, ROW_NUMBER() OVER(PARTITION BY SUBSTRING(RE.cod_ren_contrato, 1, 26)) int_ren_ordercontrato
	, CONCAT(TRIM(RE.cod_ren_tipo_ajuste),TRIM(RE.cod_ren_origen_inf)) cod_ren_internegocio
	, IF(SUBSTRING(RE.cod_ren_contrato,11,4) = '', NULL, SUBSTRING(RE.cod_ren_contrato,11,4)) cod_ren_subprodu
	, IF(RE.cod_ren_contenido IN ('CCL','CTB'), RE.cod_ren_cliente_rorac , RE.cod_ren_contrato_rorac) cod_ren_cto_rorac
FROM bi_corp_common.bt_ren_result RE
WHERE RE.partition_date = '2020-10-31' 
	AND RE.cod_ren_contenido IN ('PRE','CRE','ARF','CCO','COM','CTB','ROF','CCL') 
	AND RE.cod_ren_cta_resultados_niv_6 != '1073'
	AND RE.cod_ren_producto_niv_3 IN ('BG1','BG3') ;

---------------------------------------------------TEMP_PERIMETRO_PASIVO
SET mapred.job.queue.name=root.dataeng;
--------------------------------------------------------
SELECT RE.cod_ren_contenido, COUNT(1) Q
FROM bi_corp_common.bt_ren_result RE
WHERE RE.partition_date = '2020-10-31' 
	AND RE.cod_ren_contenido IN ('COM','CTB','ROF')
	AND RE.cod_ren_contrato = '-99100'
	AND SUBSTRING(RE.cod_ren_producto_gest, 1, 2) = 'C+'
GROUP BY RE.cod_ren_contenido ;


--__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/__/

/*  
set hive.execution.engine=spark;
set spark.yarn.queue=root.dataeng;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
----------------------------------------------------TEMP_PRODUCTO_MIS
CREATE TEMPORARY TABLE IF NOT EXISTS producto_mis AS
SELECT TRIM(cod_valor) cod_producto_gest
	, TRIM(cod_valor_padre) cod_producto_mis -- 20201013
	, TRIM(num_nivel) num_nivel
FROM bi_corp_staging.rio157_ms0_dm_dwh_composicion_jerqs 
WHERE partition_date = '2020-10-09'
	AND cod_jerarquia = 'JBP01'
	AND cod_dimension = 'PR01' ;

----------------------------------------------------TEMP_TABLON_IFRS9
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

---------------------------------------------------TEMP_ROSETTA_KEY
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

---------------------------------------------------TEMP_RESULT_ACTIVO
-- DROP TABLE perimetro_activo ;
-- SELECT COUNT(1) FROM perimetro_activo ; -- 395.846.441
--SET mapred.job.queue.name=root.dataeng;
set hive.execution.engine=spark;
set spark.yarn.queue=root.dataeng;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
--------------------------------------------------------
CREATE TEMPORARY TABLE IF NOT EXISTS perimetro_ap AS 
SELECT RE.cod_ren_contrato
	, RE.cod_ren_cliente_rorac
	, RE.cod_ren_contrato_rorac
	, RE.cod_ren_pers_ods
	, RE.cod_ren_contenido
	, RE.cod_ren_area_negocio
	, RE.cod_ren_producto_niv_3
	, date_format(RE.dt_ren_altacontrato, 'dd-MM-yyyy') dt_ren_fec_altacto 
	, date_format(RE.dt_ren_vencontrato, 'dd-MM-yyyy') dt_ren_fec_ven 
	, RE.cod_ren_entidad_espana 
	, RE.cod_ren_centro_cont cod_ren_centrocont
	, IF(SUBSTRING(RE.cod_ren_contrato,11,4) = '', NULL, SUBSTRING(RE.cod_ren_contrato,11,4)) cod_ren_subprodu
	, RE.cod_ren_gestor_prod cod_ren_gestorprod
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
	, IF(RE.dt_ren_reestruccontrato IS NULL, '01-01-9999', date_format(RE.dt_ren_reestruccontrato, 'dd-MM-yyyy')) dt_ren_fec_reestruc
	, date_format(RE.partition_date, 'dd-MM-yyyy') dt_ren_fec_extrdatos
	, ROW_NUMBER() OVER(PARTITION BY SUBSTRING(RE.cod_ren_contrato, 1, 26)) int_ren_ordercontrato
	----------------------------
	, SUM(RE.fc_ren_resultado_total_ml) fc_ren_resultado_total_ml
	, SUM(RE.fc_ren_saldo_medio_ml) fc_ren_saldo_medio_ml
	, SUM(RE.fc_ren_sdo_med_cap_ml) fc_ren_sdo_med_cap_ml
	, SUM(RE.fc_ren_sdo_med_int_ml) fc_ren_sdo_med_int_ml
	, SUM(RE.fc_ren_sdo_med_reajuste_ml) fc_ren_sdo_med_reajuste_ml
	, SUM(RE.fc_ren_sdo_med_insolv_ml) fc_ren_sdo_med_insolv_ml
	, SUM(RE.fc_ren_ing_per_ml) fc_ren_ing_per_ml
	, SUM(RE.fc_ren_ing_cap_ml) fc_ren_ing_cap_ml
	, SUM(RE.fc_ren_resultado_total_real_ml) fc_ren_resultado_total_real_ml
	---------------------------
	, IF(RE.cod_ren_contenido IN ('PRE','CRE','ARF','CCO'), 'A', 'P') cod_activo_pasivo
	--------------------------
	, RE.partition_date 
FROM bi_corp_common.bt_ren_result RE
WHERE RE.partition_date = '2020-10-31'
	AND RE.cod_ren_contenido IN ('PRE','CRE','ARF','CCO','PLZ','CTA') 
GROUP BY RE.cod_ren_contrato
	, RE.cod_ren_cliente_rorac
	, RE.cod_ren_contrato_rorac
	, RE.cod_ren_pers_ods
	, RE.cod_ren_contenido
	, RE.cod_ren_area_negocio
	, RE.cod_ren_producto_niv_3
	, date_format(RE.dt_ren_altacontrato, 'dd-MM-yyyy')  
	, date_format(RE.dt_ren_vencontrato, 'dd-MM-yyyy')  
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
	, IF(RE.dt_ren_reestruccontrato IS NULL, '01-01-9999', date_format(RE.dt_ren_reestruccontrato, 'dd-MM-yyyy'))
	, date_format(RE.partition_date, 'dd-MM-yyyy') 
	, IF(RE.cod_ren_contenido IN ('PRE','CRE','ARF','CCO'), 'A', 'P')
	, RE.partition_date ;
--------------------------------------------------------
 -- SELECT COUNT(1) FROM perimetro_pg ;

set hive.execution.engine=spark;
set spark.yarn.queue=root.dataeng;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
--------------------------------------------------------
CREATE TEMPORARY TABLE IF NOT EXISTS perimetro_pg AS 
SELECT RE.cod_ren_contrato
	, RE.cod_ren_cliente_rorac
	, RE.cod_ren_contrato_rorac
	, RE.cod_ren_pers_ods
	, RE.cod_ren_contenido
	, RE.cod_ren_area_negocio
	, RE.cod_ren_producto_niv_3
	, date_format(RE.dt_ren_altacontrato, 'dd-MM-yyyy') dt_ren_fec_altacto 
	, date_format(RE.dt_ren_vencontrato, 'dd-MM-yyyy') dt_ren_fec_ven 
	, RE.cod_ren_entidad_espana 
	, RE.cod_ren_centro_cont cod_ren_centrocont
	, IF(SUBSTRING(RE.cod_ren_contrato,11,4) = '', NULL, SUBSTRING(RE.cod_ren_contrato,11,4)) cod_ren_subprodu
	, RE.cod_ren_gestor_prod cod_ren_gestorprod
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
	, IF(RE.dt_ren_reestruccontrato IS NULL, '01-01-9999', date_format(RE.dt_ren_reestruccontrato, 'dd-MM-yyyy')) dt_ren_fec_reestruc
	, date_format(RE.partition_date, 'dd-MM-yyyy') dt_ren_fec_extrdatos
	, NULL int_ren_ordercontrato
	----------------------------
	, SUM(RE.fc_ren_resultado_total_ml) fc_ren_resultado_total_ml
	, SUM(RE.fc_ren_saldo_medio_ml) fc_ren_saldo_medio_ml
	, SUM(RE.fc_ren_sdo_med_cap_ml) fc_ren_sdo_med_cap_ml
	, SUM(RE.fc_ren_sdo_med_int_ml) fc_ren_sdo_med_int_ml
	, SUM(RE.fc_ren_sdo_med_reajuste_ml) fc_ren_sdo_med_reajuste_ml
	, SUM(RE.fc_ren_sdo_med_insolv_ml) fc_ren_sdo_med_insolv_ml
	, SUM(RE.fc_ren_ing_per_ml) fc_ren_ing_per_ml
	, SUM(RE.fc_ren_ing_cap_ml) fc_ren_ing_cap_ml
	, SUM(RE.fc_ren_resultado_total_real_ml) fc_ren_resultado_total_real_ml
	---------------------------
	, IF(SUBSTRING(RE.cod_ren_producto_gest, 1, 2) = 'C+', 'A', IF(SUBSTRING(RE.cod_ren_producto_gest, 1, 2) = 'C-', 'P', NULL)) cod_activo_pasivo
	--------------------------
	, RE.partition_date 
FROM bi_corp_common.bt_ren_result RE
WHERE RE.partition_date = '2020-10-31'
	AND RE.cod_ren_contenido IN ('COM','CTB','ROF','CCL') 
	AND RE.cod_ren_contrato = '-99100'
GROUP BY RE.cod_ren_contrato
	, RE.cod_ren_cliente_rorac
	, RE.cod_ren_contrato_rorac
	, RE.cod_ren_pers_ods
	, RE.cod_ren_contenido
	, RE.cod_ren_area_negocio
	, RE.cod_ren_producto_niv_3
	, date_format(RE.dt_ren_altacontrato, 'dd-MM-yyyy')  
	, date_format(RE.dt_ren_vencontrato, 'dd-MM-yyyy')  
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
	, IF(RE.dt_ren_reestruccontrato IS NULL, '01-01-9999', date_format(RE.dt_ren_reestruccontrato, 'dd-MM-yyyy'))
	, date_format(RE.partition_date, 'dd-MM-yyyy') 
	, IF(SUBSTRING(RE.cod_ren_producto_gest, 1, 2) = 'C+', 'A', IF(SUBSTRING(RE.cod_ren_producto_gest, 1, 2) = 'C-', 'P', NULL))
	, RE.partition_date ;
--------------------------------------------------------

set hive.execution.engine=spark;
set spark.yarn.queue=root.dataeng;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
--------------------------------------------------------
CREATE TEMPORARY TABLE IF NOT EXISTS perimetro_ct AS 
SELECT RE.cod_ren_contrato
	, RE.cod_ren_cliente_rorac
	, RE.cod_ren_contrato_rorac
	, RE.cod_ren_pers_ods
	, RE.cod_ren_contenido
	, RE.cod_ren_area_negocio
	, RE.cod_ren_producto_niv_3
	, date_format(RE.dt_ren_altacontrato, 'dd-MM-yyyy') dt_ren_fec_altacto 
	, date_format(RE.dt_ren_vencontrato, 'dd-MM-yyyy') dt_ren_fec_ven 
	, RE.cod_ren_entidad_espana 
	, RE.cod_ren_centro_cont cod_ren_centrocont
	, IF(SUBSTRING(RE.cod_ren_contrato,11,4) = '', NULL, SUBSTRING(RE.cod_ren_contrato,11,4)) cod_ren_subprodu
	, RE.cod_ren_gestor_prod cod_ren_gestorprod
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
	, IF(RE.dt_ren_reestruccontrato IS NULL, '01-01-9999', date_format(RE.dt_ren_reestruccontrato, 'dd-MM-yyyy')) dt_ren_fec_reestruc
	, date_format(RE.partition_date, 'dd-MM-yyyy') dt_ren_fec_extrdatos
	, NULL int_ren_ordercontrato
	----------------------------
	, SUM(RE.fc_ren_resultado_total_ml) fc_ren_resultado_total_ml
	, SUM(RE.fc_ren_saldo_medio_ml) fc_ren_saldo_medio_ml
	, SUM(RE.fc_ren_sdo_med_cap_ml) fc_ren_sdo_med_cap_ml
	, SUM(RE.fc_ren_sdo_med_int_ml) fc_ren_sdo_med_int_ml
	, SUM(RE.fc_ren_sdo_med_reajuste_ml) fc_ren_sdo_med_reajuste_ml
	, SUM(RE.fc_ren_sdo_med_insolv_ml) fc_ren_sdo_med_insolv_ml
	, SUM(RE.fc_ren_ing_per_ml) fc_ren_ing_per_ml
	, SUM(RE.fc_ren_ing_cap_ml) fc_ren_ing_cap_ml
	, SUM(RE.fc_ren_resultado_total_real_ml) fc_ren_resultado_total_real_ml
	---------------------------
	, NULL cod_activo_pasivo
	--------------------------
	, RE.partition_date 
FROM bi_corp_common.bt_ren_result RE
WHERE RE.partition_date = '2020-10-31'
	AND RE.cod_ren_contenido IN ('COM','CTB','ROF','CCL') 
	AND RE.cod_ren_contrato != '-99100'
GROUP BY RE.cod_ren_contrato
	, RE.cod_ren_cliente_rorac
	, RE.cod_ren_contrato_rorac
	, RE.cod_ren_pers_ods
	, RE.cod_ren_contenido
	, RE.cod_ren_area_negocio
	, RE.cod_ren_producto_niv_3
	, date_format(RE.dt_ren_altacontrato, 'dd-MM-yyyy')  
	, date_format(RE.dt_ren_vencontrato, 'dd-MM-yyyy')  
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
	, IF(RE.dt_ren_reestruccontrato IS NULL, '01-01-9999', date_format(RE.dt_ren_reestruccontrato, 'dd-MM-yyyy'))
	, date_format(RE.partition_date, 'dd-MM-yyyy') 
	, RE.partition_date ;
---------------------------------------------------
*/
-- DROP TABLE result_cto ;
set hive.execution.engine=spark;
set spark.yarn.queue=root.dataeng;
CREATE TEMPORARY TABLE IF NOT EXISTS result_cto AS
SELECT DISTINCT RE.cod_ren_contrato , RE.cod_ren_contenido , RE.cod_ren_contrato_rorac , IF(RE.cod_ren_contenido IN ('PLZ','CTA'), 'P', IF(RE.cod_ren_contenido IN ('PRE','CRE','ARF','CCO'), 'A', NULL)) perimetro
FROM bi_corp_common.bt_ren_result RE
WHERE RE.partition_date = '2020-10-31'
AND RE.cod_ren_contenido IN ('PRE','CRE','ARF','CCO','PLZ','CTA','COM','CTB','ROF','CCL')
AND RE.cod_ren_contrato != '-99100'
ORDER BY RE.cod_ren_contrato , cod_ren_contenido , RE.cod_ren_contrato_rorac ;

-- perimetro|cant    |
-- ---------|--------|
--          |13417569|
-- A        |53256516|
-- P        | 8866625|

-- SELECT perimetro , count(1) cant FROM result_cto GROUP BY perimetro ;

-- BUSCO PERIMETRO DE CONTRATOS
---------------------------------------
set hive.execution.engine=spark;
set spark.yarn.queue=root.dataeng;
---------------------------------------
SELECT perimetro , count(1) cant FROM (
SELECT A.cod_ren_contrato , A.cod_ren_contenido , A.cod_ren_contrato_rorac  , IF(A.perimetro IS NULL, B.perimetro, A.perimetro) perimetro
FROM result_cto A
	LEFT JOIN result_cto B 
		ON A.cod_ren_contrato = B.cod_ren_contrato
		AND B.perimetro IS NOT NULL 
)C GROUP BY perimetro ;

SELECT * FROM result_cto WHERE cod_Ren_contrato = '00720000052001000000216096000'