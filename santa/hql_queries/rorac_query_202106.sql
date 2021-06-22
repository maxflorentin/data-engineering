
SHOW PARTITIONS bi_corp_staging.malpe_ptb_pedt030 ;

set mapred.job.queue.name=root.dataeng ;
set hive.exec.dynamic.partition.mode=nonstrict ;

WITH deu_adic AS (
SELECT 'D.ADIC' cod_cys_interface
	, CAST(rda.num_persona AS BIGINT) cod_per_nup
	, TRIM(rda.cod_entidad) cod_cys_entidad
	, CAST(rda.cod_centro AS INT) cod_suc_sucursal
	, CAST(rda.num_cuenta AS BIGINT) cod_nro_cuenta
	, TRIM(rda.cod_producto) cod_prod_producto
	, TRIM(rda.cod_subprodu) cod_prod_subproducto
	, TRIM(rda.cod_divisa) cod_div_divisa
	, TRIM(rda.num_identificacion) int_cys_identificador
	, NULL cod_cys_tipodocumento
	, NULL per_num_doc
	, NULL dt_cys_fechaalta
	, NULL dt_cys_fechavto
	, NULL cod_cys_mipymecli
	, NULL cod_cys_mipymecto
	, NULL cod_cys_tipocliente
	, NULL cod_cys_segmento
	, NULL cod_cys_destinofondo
	, TRIM(rda.cod_concepto) cod_cys_concepto
	, IF(TRIM(rda.tip_gtia) = '', NULL, TRIM(rda.tip_gtia)) cod_cys_tipogarantia
	, IF(TRIM(rda.cod_gtia) = '', NULL, TRIM(rda.cod_gtia)) cod_cys_garantia
	, NULL cod_cys_tipocartera
	, NULL cod_cys_familiarubro
	, NULL cod_cys_situacionbcra 
	, TRIM(rda.cod_rubro_altair) cod_cys_rubroaltair
	, TRIM(rda.cod_rubro_bcra) cod_cys_rubrobcra
	, TRIM(rda.cod_rubro_cargabal) cod_cys_rubrocargabal
	, NULL dt_cys_fechapriminc
	, NULL int_cys_diasatraso
	, NULL int_cys_tramo
	, rda.imp_deuda fc_cys_importedeuda
	, rda.imp_prev fc_cys_importeprovision
	, NULL fc_cys_montooriginal
	, NULL cod_cys_tasa
	, NULL fc_cys_valortasaoper
	, NULL cod_cys_tipopaquete
	, NULL cod_cys_prodpaquete
	, NULL cod_cys_sprodpaquete
	, NULL cod_cys_prodoriginal
	, NULL cod_cys_sprodoriginal
	, NULL dt_cys_fechaupgradepaq
	, NULL dt_cys_fechaaltaprod
	, NULL dt_cys_fechavtoprod
	, NULL cod_cys_marcaaltair
	, NULL cod_cys_submarcaaltair
	, NULL dt_cys_fechaingmoria
	, NULL dt_cys_fechaingemerix
	, NULL dt_cys_fechaprimercuota
	, NULL int_cys_plazo
	, NULL cod_cys_proced
	, NULL cod_cys_canal
	, NULL cod_cys_sucursalorigen
	, NULL cod_cys_campana
	, NULL int_cys_numshot
	, NULL cod_cys_castigocontrato
	, NULL cod_cys_castigocliente
	, NULL cod_cys_entidadcta
	, NULL cod_suc_sucursalcuenta
	, NULL cod_nro_cuentacto
	, NULL cod_prod_productocta
	, NULL cod_prod_subproductocta
	, NULL cod_div_divisacta
	, TRIM(s.segmento_control) ds_cys_descripcionsegmento
	, IF(TRIM(s.segmento) = 'INDIVIDUOS', TRIM(p.categoria_particulares), TRIM(p.categoria_carterizados)) ds_cys_categoriaproducto	
	, TRIM(s.detalle_renta) as ds_cys_detallerenta
	, NULL ds_cys_bucket
	, last_day(to_date(CONCAT(SUBSTRING(rda.data_date_part, 1, 4),'-',SUBSTRING(rda.data_date_part, 5, 2),'-01'))) partition_date
FROM santander_business_risk_arda.contratos_deudores_adic rda
LEFT JOIN bi_corp_staging.malpe_ptb_pedt030 p30 
	ON CAST(rda.num_persona AS BIGINT) = CAST(p30.penumper AS BIGINT)
	AND p30.partition_date = '2021-02-26' -- '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_prevmonth_malpe_pedt030', dag_id='?') }}'
LEFT JOIN bi_corp_staging.risksql5_segmentos s 
	ON TRIM(p30.pesegcla) = TRIM(s.ramo) 
	AND s.fecha_informacion = last_day(to_date(CONCAT(SUBSTRING(rda.data_date_part, 1, 4),'-',SUBSTRING(rda.data_date_part, 5, 2),'-01'))) 
LEFT JOIN bi_corp_staging.risksql5_productos p 
	ON TRIM(rda.cod_producto)  = TRIM(p.codigo_producto) 
	AND TRIM(rda.cod_subprodu) = TRIM(p.codigo_subproducto)  
	AND p.fecha_informacion = last_day(to_date(CONCAT(SUBSTRING(rda.data_date_part, 1, 4),'-',SUBSTRING(rda.data_date_part, 5, 2),'-01')))
WHERE rda.data_date_part = '20210226' ) -- '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_last_working_day_arda', dag_id='?') }}'

INSERT OVERWRITE TABLE bi_corp_common.stk_cys_adsf
PARTITION(partition_date) 

SELECT DA.* 
FROM deu_adic DA
LEFT JOIN bi_corp_staging.rio125_adsf_cartera CA
	ON DA.cod_per_nup = CAST(CA.nup AS BIGINT) 
	AND DA.cod_nro_cuenta = CAST(CA.contrato AS BIGINT) 
	AND DA.cod_prod_producto = TRIM(CA.producto) 
	AND DA.cod_div_divisa = TRIM(CA.divisa) 
	AND DA.partition_date = CA.partition_date  
	AND CAST(CA.nup AS BIGINT) IS NULL 

UNION ALL 

SELECT TRIM(CA.id_interface) cod_cys_interface
	, CAST(CA.nup AS BIGINT) cod_per_nup
	, TRIM(CA.entidad) cod_cys_entidad
	, CAST(CA.centro AS INT) cod_suc_sucursal
	, CAST(CA.contrato AS BIGINT) cod_nro_cuenta
	, IF(TRIM(CA.producto) = 'null', NULL, TRIM(CA.producto)) cod_prod_producto
	, TRIM(CA.subprod) cod_prod_subproducto
	, TRIM(CA.divisa) cod_div_divisa
	, TRIM(CA.num_identif) int_cys_identificador
	, IF(TRIM(CA.tip_doc) = 'null', NULL, TRIM(CA.tip_doc)) cod_cys_tipodocumento
	, CAST(CA.num_doc AS BIGINT) per_num_doc
	, SUBSTRING(CA.fec_alta_cto, 1, 10) dt_cys_fechaalta 
	, SUBSTRING(CA.fec_vto_cto, 1, 10) dt_cys_fechavto
	, IF(TRIM(CA.mca_mipyme_cli) = 'null', NULL, TRIM(CA.mca_mipyme_cli)) cod_cys_mipymecli
	, IF(TRIM(CA.mca_mipyme_cto) = 'null', NULL, TRIM(CA.mca_mipyme_cto))  cod_cys_mipymecto
	, IF(TRIM(CA.tipo_cliente) = 'null', NULL, TRIM(CA.tipo_cliente)) cod_cys_tipocliente
	, IF(TRIM(CA.segmento) = 'null', NULL, TRIM(CA.segmento)) cod_cys_segmento
	, IF(TRIM(CA.cod_dest_fondo) = 'null', NULL, TRIM(CA.cod_dest_fondo)) cod_cys_destinofondo
	, TRIM(CA.cod_concepto) cod_cys_concepto
	, TRIM(CA.tipo_gtia) cod_cys_tipogarantia
	, TRIM(CA.cod_gtia) cod_cys_garantia
	, IF(TRIM(CA.tipo_cartera) = 'null', NULL, TRIM(CA.tipo_cartera)) cod_cys_tipocartera
	, TRIM(CA.familia_rubro) cod_cys_familiarubro
	, TRIM(CA.sit_bcra) cod_cys_situacionbcra
	, TRIM(CA.rubro_altair) cod_cys_rubroaltair
	, TRIM(CA.rubro_bcra) cod_cys_rubrobcra
	, TRIM(CA.rubro_cargabal) cod_cys_rubrocargabal
	, IF(SUBSTRING(CA.fec_prim_inc, 1, 10) = '0001-01-01', NULL, SUBSTRING(CA.fec_prim_inc, 1, 10)) dt_cys_fechapriminc
	, CAST(CA.dias_atraso AS INT) int_cys_diasatraso
	, CAST(CA.tramo AS INT) int_cys_tramo
	, TRIM(CA.imp_deuda) fc_cys_importedeuda
	, TRIM(CA.imp_prevision) fc_cys_importeprovision
	, TRIM(CA.monto_original) fc_cys_montooriginal
	, TRIM(CA.tasa) cod_cys_tasa
	, TRIM(CA.valor_tasa_oper) fc_cys_valortasaoper
	, IF(TRIM(CA.tipo_paquete) = 'null', NULL, TRIM(CA.tipo_paquete)) cod_cys_tipopaquete
	, IF(TRIM(CA.prod_paquete) = 'null', NULL, TRIM(CA.prod_paquete)) cod_cys_prodpaquete
	, IF(TRIM(CA.sprod_paquete) = 'null', NULL, TRIM(CA.sprod_paquete)) cod_cys_sprodpaquete
	, IF(TRIM(CA.prod_original) = 'null', NULL, TRIM(CA.prod_original)) cod_cys_prodoriginal
	, IF(TRIM(CA.sprod_original) = 'null', NULL, TRIM(CA.sprod_original)) cod_cys_sprodoriginal
	, IF(TRIM(SUBSTRING(CA.fec_upgra_paq, 1, 10)) = 'null', NULL, TRIM(SUBSTRING(CA.fec_upgra_paq, 1, 10))) dt_cys_fechaupgradepaq	
	, TRIM(SUBSTRING(CA.fec_alta_prod, 1, 10)) dt_cys_fechaaltaprod
	, TRIM(SUBSTRING(CA.fec_vto_prod, 1, 10)) dt_cys_fechavtoprod	
	, TRIM(CA.marca_altair) cod_cys_marcaaltair
	, TRIM(CA.submarca_altair) cod_cys_submarcaaltair
	, IF(SUBSTRING(CA.fec_ing_moria, 1, 10) = '0001-01-01', NULL, SUBSTRING(CA.fec_ing_moria, 1, 10)) dt_cys_fechaingmoria
	, IF(SUBSTRING(CA.fec_ing_emerix, 1, 10) = 'null', NULL, SUBSTRING(CA.fec_ing_emerix, 1, 10)) dt_cys_fechaingemerix 
	, IF(SUBSTRING(CA.fec_prim_cuota, 1, 10) = 'null', NULL, SUBSTRING(CA.fec_prim_cuota, 1, 10)) dt_cys_fechaprimercuota
	, CAST(CA.plazo AS INT) int_cys_plazo
	, IF(TRIM(CA.cod_proced) = 'null', NULL, TRIM(CA.cod_proced)) cod_cys_proced
	, IF(TRIM(CA.cod_canal) = 'null', NULL, TRIM(CA.cod_canal)) cod_cys_canal
	, IF(TRIM(CA.cod_suc_origen) = 'null', NULL, TRIM(CA.cod_suc_origen)) cod_cys_sucursalorigen
	, IF(TRIM(CA.num_campana) = 'null', NULL, TRIM(CA.num_campana)) cod_cys_campana
	, CAST(CA.num_shot AS INT) int_cys_numshot
	, TRIM(CA.castigo_contrato) cod_cys_castigocontrato
	, TRIM(CA.castigo_cliente) cod_cys_castigocliente
	, IF(TRIM(CA.entidad_cta) = 'null', NULL, TRIM(CA.entidad_cta)) cod_cys_entidadcta
	, IF(TRIM(CA.centro_cta) = 'null', NULL, CAST(TRIM(CA.centro_cta) AS INT)) cod_suc_sucursalcuenta
	, IF(TRIM(CA.contrato_cta) = 'null', NULL, CAST(TRIM(CA.contrato_cta) AS INT)) cod_nro_cuentacto
	, IF(TRIM(CA.producto_cta) = 'null', NULL, TRIM(CA.producto_cta)) cod_prod_productocta
	, IF(TRIM(CA.subprod_cta) = 'null', NULL, TRIM(CA.subprod_cta)) cod_prod_subproductocta
	, IF(TRIM(CA.divisa_cta) = 'null', NULL, TRIM(CA.divisa_cta)) cod_div_divisacta
	, TRIM(s.segmento_control) ds_cys_descripcionsegmento
	, IF(TRIM(s.segmento) = 'INDIVIDUOS', TRIM(p.categoria_particulares), TRIM(p.categoria_carterizados)) ds_cys_categoriaproducto	
	, TRIM(s.detalle_renta) as ds_cys_detallerenta
	, CASE WHEN COALESCE(CAST(dias_atraso AS INT),0) = 0 THEN '0' 
		WHEN CAST(dias_atraso AS INT) BETWEEN 1 AND 30 THEN '1-30' 
		WHEN CAST(dias_atraso AS INT) BETWEEN 31 AND 60 THEN '31-60' 
		WHEN CAST(dias_atraso AS INT) BETWEEN 61 AND 90 THEN '61-90' 
		WHEN CAST(dias_atraso AS INT) BETWEEN 91 AND 120 THEN '91-120' 
		WHEN CAST(dias_atraso AS INT) BETWEEN 121 AND 150 THEN '121-150' 
		WHEN CAST(dias_atraso AS INT) BETWEEN 151 AND 180 THEN '151-180' ELSE 'MAS DE 180' END ds_cys_bucket
	, last_day(to_date(CONCAT(SUBSTRING(CA.periodo,1, 4),'-',SUBSTRING(CA.periodo,5, 2),'-01'))) partition_date 
FROM bi_corp_staging.rio125_adsf_cartera CA 
LEFT JOIN bi_corp_staging.malpe_ptb_pedt030 p30 
	ON CAST(CA.nup AS BIGINT) = CAST(p30.penumper AS BIGINT)
	AND p30.partition_date = '2021-02-26' -- '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_prevmonth_malpe_pedt030', dag_id='?') }}'
LEFT JOIN bi_corp_staging.risksql5_segmentos s 
	ON TRIM(p30.pesegcla) = TRIM(s.ramo) 
	AND s.fecha_informacion = last_day(to_date(CONCAT(SUBSTRING(CA.periodo,1, 4),'-',SUBSTRING(CA.periodo,5, 2),'-01'))) 
LEFT JOIN bi_corp_staging.risksql5_productos p 
	ON TRIM(CA.producto) = TRIM(p.codigo_producto) 
	AND TRIM(CA.subprod) = TRIM(p.codigo_subproducto)  
	AND p.fecha_informacion = last_day(to_date(CONCAT(SUBSTRING(CA.periodo,1, 4),'-',SUBSTRING(CA.periodo,5, 2),'-01')))
WHERE CA.partition_date = '2021-02-28' ; -- '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_to', dag_id='?') }}'



















DROP TABLE IF EXISTS bi_corp_staging.rorac_contratos_ap ;  
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rorac_contratos_ap ( 
	cod_ren_unidad	string ,
	dt_ren_data	string ,
	cod_ren_finalidaddatos	int ,
	cod_ren_contrato_rorac	string ,
	cod_ren_contrato	string ,
	cod_ren_areanegocio	string ,
	cod_ren_divisa	string ,
	cod_ren_division	string ,
	cod_ren_producto	string ,
	cod_ren_pers_ods	string ,
	fc_ren_margenmes	decimal(30,4) ,
	fc_ren_sdbsmv	decimal(30,4) ,
	fc_ren_sfbsmv	decimal(33,4) ,
	dt_ren_fec_altacto	string ,
	dt_ren_fec_ven	string ,
	cod_ren_entidad_espana	string ,
	cod_ren_centrocont	string ,
	cod_ren_subprodu	string ,
	cod_ren_zona	string ,
	cod_ren_territorial	string ,
	cod_ren_gestorprod	string ,
	cod_ren_gestor	string ,
	id_cto_bdr	string ,
	fc_ren_sdbsfm	decimal(35,4) ,
	fc_ren_interes	decimal(30,4) ,
	fc_ren_comfin	decimal(30,4) ,
	fc_ren_comnofin	decimal(30,4) ,
	cod_ren_vincula	string ,
	fc_ren_rof	decimal(30,4) ,
	fc_ren_tti	decimal(30,4) ,
	cod_ren_mtm	string ,
	cod_ren_nocional	string ,
	cod_ren_divisa_mis	string ,
	flag_ren_moralocal	string ,
	flag_ren_carterizadas	string ,
	ds_ren_nombrecliente	string ,
	cod_per_tipopersona	string ,
	cod_ren_nifcif	string ,
	cod_ren_origen_inf	string ,
	cod_ren_internegocio	string ,
	cod_ren_areanegociocorp	string ,
	cod_productogest	string ,
	cod_segmentogest	string ,
	cod_producto_operacional	string ,
	ds_ren_ficheromis	string ,
	dt_ren_fec_reestruc	string ,
	dt_ren_fec_extrdatos	string ,
	flag_ren_neteo	int ,
	fc_ren_orex	decimal(30,4) ,
	fc_ifrs_provision	decimal(38,15) ,
	fc_ren_costemensualcto	int ,
	cod_nivel_operacion	int ,
	cod_ren_idru	int ,
	perimetro string ,
	partition_date	string ) 
 STORED AS PARQUET 
 LOCATION '/santander/bi-corp/staging/rio157fact/rorac_contratos_ap'
 
 
 
 
 
 
 
 
 
 
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

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

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

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

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

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

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

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

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

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

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

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

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

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

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

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

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

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

DROP TABLE IF EXISTS ru ;	
CREATE TEMPORARY TABLE ru AS	
WITH rs_activo AS (

	SELECT DISTINCT s9.cod_ren_pers_ods
		, s9.cod_producto 
		, s9.cod_subprodu 
		, s9.cod_ren_segmento_gest cod_segmentogest
		, s9.cod_ren_divisa_mis
	FROM contratos_s9 s9
	WHERE TRIM(s9.cod_ren_pers_ods) != '-99100'
		AND s9.cod_subprodu IS NOT NULL
	
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
		ON regexp_replace(RE.cod_ren_pers_ods, '10072', '') = TRIM(SE.penumper)
		
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
        , CASE WHEN pesegcal IN ("A", "B", "C", "S") AND cod_baremo_global = "0006" THEN 1200009 
	         WHEN pesegcal IN ("A", "B", "C", "S") AND cod_baremo_global IN ("0013", "0182", "0183") THEN 1200041
	         WHEN pesegcal IN ("A", "B", "C", "S") AND cod_baremo_global = "0017" THEN 1200073
	         WHEN pesegcal IN ("A", "B", "C", "S") AND cod_baremo_global NOT IN ("0017", "0006", "0013", "0182", "0183") THEN 1200105
	         WHEN pesegcal IN ("P1", "C1") THEN 1200137
	         WHEN pesegcal = "P2" THEN 1200169
	         WHEN pesegcal IN ("EM", "G1", "G2") THEN 1200201
	         WHEN pesegcal IN ("IU", "IP") THEN 1200233
	         WHEN cod_baremo_local = 1 AND pesegcal IN ("F2", "OF", "GL", "LA", "LO", "MU", "OT", "FI", "F1") THEN 1040176
	         WHEN cod_baremo_local = 11 AND pesegcal IN ("F2", "OF", "GL", "LA", "LO", "MU", "OT", "FI", "F1") THEN	1200290
	         WHEN cod_baremo_local = 17 AND pesegcal IN ("F2", "OF", "GL", "LA", "LO", "MU", "OT", "FI", "F1") THEN	1200322
	         WHEN pesegcal = "PU" AND cod_ren_divisa_mis = "ARS"  THEN 1200354
	         WHEN pesegcal = "PU" AND cod_ren_divisa_mis <> "ARS"  THEN 1040181 ELSE 9999999 END id_ru 
	    , pesegcal 
	    , cod_baremo_local
	    , cod_baremo_global
	    , cod_producto
		, cod_subprodu
		, cod_segmentogest
		, cod_ren_divisa_mis
	FROM rs_baremo ;

DROP TABLE IF EXISTS contratos_s10 ;
CREATE TEMPORARY TABLE contratos_s10 AS
SELECT MAX(s9.cod_ren_contrato_rorac) cod_ren_contrato_rorac
	, MAX(s9.id_cto_bdr) id_cto_bdr
	, s9.cod_ren_contenido
	, s9.cod_ren_contrato
	, s9.cod_ren_area_negocio
	, MAX(s9.cod_ren_producto_gest) cod_ren_producto_gest
	, MAX(s9.fc_ren_margenmes) fc_ren_margenmes
	, MAX(s9.fc_ren_sdbsmv) fc_ren_sdbsmv
	, MAX(s9.fc_ren_sfbsmv) fc_ren_sfbsmv
	, MAX(s9.fc_ren_sdbsfm) fc_ren_sdbsfm
	, MAX(s9.fc_ren_interes) fc_ren_interes
	, MAX(s9.fc_ren_comfin) fc_ren_comfin
	, MAX(s9.fc_ren_comnofin) fc_ren_comnofin
	, MAX(s9.fc_ren_rof) fc_ren_rof
	, MAX(s9.fc_ren_tti) fc_ren_tti
	, MAX(s9.fc_ren_orex) fc_ren_orex
	, MAX(s9.cod_ren_pers_ods) cod_ren_pers_ods
	, MAX(s9.dt_ren_altacontrato) dt_ren_altacontrato
	, MAX(s9.dt_ren_vencontrato) dt_ren_vencontrato
	, MAX(s9.dt_ren_reestruccontrato) dt_ren_reestruccontrato
	, MAX(s9.cod_ren_entidad_espana) cod_ren_entidad_espana
	, MAX(s9.cod_ren_centro_cont) cod_ren_centro_cont
	, MAX(s9.cod_ren_gestor_prod) cod_ren_gestor_prod
	, MAX(s9.cod_ren_gestor) cod_ren_gestor
	, MAX(s9.cod_ren_vincula) cod_ren_vincula
	, MAX(s9.cod_ren_divisa_mis) cod_ren_divisa_mis
	, MAX(s9.ds_per_nombre_apellido) ds_per_nombre_apellido
	, MAX(s9.cod_per_tipopersona) cod_per_tipopersona
	, MAX(s9.cod_ren_origen_inf) cod_ren_origen_inf
	, MAX(s9.cod_ren_internegocio) cod_ren_internegocio
	, MAX(s9.cod_ren_segmento_gest) cod_ren_segmento_gest
	, MAX(s9.cod_producto) cod_producto
	, MAX(s9.cod_subprodu) cod_subprodu
	, MAX(s9.int_orden) int_orden
	, MAX(s9.partition_date) partition_date
	, MAX(s9.cod_ren_division) cod_ren_division
	, MAX(s9.cod_ren_producto) cod_ren_producto
	, MAX(s9.cod_ren_area_negociocorp) cod_ren_area_negociocorp
	, MAX(s9.perimetro) perimetro
	, MAX(s9.fc_ifrs_provision) fc_ifrs_provision
	, MAX(RU.id_ru) id_ru
FROM contratos_s9 s9
LEFT JOIN ru RU 
	ON RU.cod_ren_pers_ods = s9.cod_ren_pers_ods 
GROUP BY s9.cod_ren_contenido
	, s9.cod_ren_contrato
	, s9.cod_ren_area_negocio ;

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

DROP TABLE IF EXISTS contratos_s11 ;
CREATE TEMPORARY TABLE contratos_s11 AS
SELECT cod_ren_contrato_rorac
	, id_cto_bdr
	, cod_ren_contenido
	, cod_ren_contrato
	, cod_ren_area_negocio
	, cod_ren_producto_gest
	, fc_ren_margenmes
	, fc_ren_sdbsmv
	, fc_ren_sfbsmv
	, fc_ren_sdbsfm
	, fc_ren_interes
	, fc_ren_comfin
	, fc_ren_comnofin
	, fc_ren_rof
	, fc_ren_tti
	, fc_ren_orex
	, cod_ren_pers_ods
	, dt_ren_altacontrato
	, dt_ren_vencontrato
	, dt_ren_reestruccontrato
	, cod_ren_entidad_espana
	, cod_ren_centro_cont
	, cod_ren_gestor_prod
	, cod_ren_gestor
	, cod_ren_vincula
	, cod_ren_divisa_mis
	, ds_per_nombre_apellido
	, cod_per_tipopersona
	, cod_ren_origen_inf
	, cod_ren_internegocio
	, cod_ren_segmento_gest
	, cod_producto
	, cod_subprodu
	, int_orden
	, partition_date
	, cod_ren_division
	, cod_ren_producto
	, cod_ren_area_negociocorp
	, perimetro
	, fc_ifrs_provision
	, CASE WHEN cod_ren_contenido IN ('CCL','CTB') AND cod_ren_area_negociocorp = 'AN01020210' AND cod_ren_divisa_mis = 'ARS' THEN 1200201
			  WHEN cod_ren_contenido IN ('CCL','CTB') AND cod_ren_area_negociocorp = 'AN01020210' AND cod_ren_divisa_mis != 'ARS' THEN 1200649
			  WHEN cod_ren_contenido IN ('CCL','CTB') AND cod_ren_area_negociocorp = 'AN01040220' AND cod_ren_divisa_mis = 'ARS' THEN 1200233
			  WHEN cod_ren_contenido IN ('CCL','CTB') AND cod_ren_area_negociocorp = 'AN01040220' AND cod_ren_divisa_mis != 'ARS' THEN 1200681
			  WHEN cod_ren_contenido IN ('CCL','CTB') AND cod_ren_area_negociocorp = 'AN01020410' AND cod_ren_divisa_mis = 'ARS' THEN 1200137
			  WHEN cod_ren_contenido IN ('CCL','CTB') AND cod_ren_area_negociocorp = 'AN01020410' AND cod_ren_divisa_mis != 'ARS' THEN 1200585
			  WHEN cod_ren_contenido IN ('CCL','CTB') AND cod_ren_area_negociocorp = 'AN01020200' AND cod_ren_divisa_mis = 'ARS' THEN 1200169
			  WHEN cod_ren_contenido IN ('CCL','CTB') AND cod_ren_area_negociocorp = 'AN01020200' AND cod_ren_divisa_mis != 'ARS' THEN 1200617
			  WHEN cod_ren_contenido IN ('CCL','CTB') AND cod_ren_area_negociocorp IN ('AN01010500','AN01010201') THEN 1200425
			ELSE COALESCE(id_ru, 9999999)  END cod_ren_idru
FROM contratos_s10 ;

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

INSERT overwrite TABLE bi_corp_staging.rorac_contratos_ap
SELECT	
	'ARG' cod_ren_unidad	,
	date_format(partition_date, 'yyyyMM') dt_ren_data	,
	1 cod_ren_finalidaddatos	,
	cod_ren_contrato_rorac	,
	cod_ren_contrato	,
	cod_ren_area_negocio	cod_ren_areanegocio ,
	'ARS' cod_ren_divisa	,
	cod_ren_division	,
	cod_ren_producto	,
	cod_ren_pers_ods	,
	fc_ren_margenmes	,
	fc_ren_sdbsmv	,
	fc_ren_sfbsmv	,
	dt_ren_altacontrato	dt_ren_fec_altacto ,
	dt_ren_vencontrato	dt_ren_fec_ven ,
	cod_ren_entidad_espana	,
	cod_ren_centro_cont	cod_ren_centrocont ,
	cod_subprodu	cod_ren_subprodu ,
	NULL cod_ren_zona	,
	NULL cod_ren_territorial	,
	cod_ren_gestor_prod	cod_ren_gestorprod ,
	cod_ren_gestor	,
	id_cto_bdr	,
	fc_ren_sdbsfm	,
	fc_ren_interes	,
	fc_ren_comfin	,
	fc_ren_comnofin	,
	cod_ren_vincula	,
	fc_ren_rof	,
	fc_ren_tti	,
	NULL cod_ren_mtm	,
	NULL cod_ren_nocional	,
	cod_ren_divisa_mis	,
	NULL flag_ren_moralocal	,
	NULL flag_ren_carterizadas	,
	ds_per_nombre_apellido ds_ren_nombrecliente	,
	cod_per_tipopersona	,
	NULL cod_ren_nifcif	,
	cod_ren_origen_inf	,
	cod_ren_internegocio	,
	cod_ren_area_negociocorp cod_ren_areanegociocorp	,
	cod_ren_producto_gest cod_productogest	,
	cod_ren_segmento_gest cod_segmentogest	,
	cod_producto cod_producto_operacional	,
	CONCAT(regexp_replace(partition_date, '-', ''), 'v1') ds_ren_ficheromis	,
	dt_ren_reestruccontrato	dt_ren_fec_reestruc ,
	date_format(partition_date, 'dd-MM-yyyy') dt_ren_fec_extrdatos	,
	IF(cod_ren_contenido = 'CCL' AND fc_ren_sdbsmv != 0, 1,IF(cod_ren_contenido = 'CCL', 2, IF(cod_ren_contenido = 'CTB' AND fc_ren_sdbsmv < 0, 2, 0))) flag_ren_neteo	,
	fc_ren_orex	,
	COALESCE(fc_ifrs_provision, 0) fc_ifrs_provision	,
	0 fc_ren_costemensualcto	,
	IF(cod_ren_contenido NOT IN ('CCL','CTB'), 0, IF(cod_ren_pers_ods != '-99100', 1, 3)) cod_nivel_operacion	,
	cod_ren_idru	,
	perimetro ,
	partition_date	
FROM contratos_s11
WHERE partition_date = '2020-12-31' ;

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE bi_corp_common.bt_ror_input_activo
PARTITION(partition_date) 
SELECT cod_ren_unidad
	, dt_ren_data
	, cod_ren_finalidaddatos
	, cod_ren_contrato_rorac
	, cod_ren_contrato
	, cod_ren_areanegocio
	, cod_ren_divisa
	, cod_ren_division
	, cod_ren_producto
	, cod_ren_pers_ods
	, fc_ren_margenmes
	, fc_ren_sdbsmv
	, fc_ren_sfbsmv
	, dt_ren_fec_altacto
	, dt_ren_fec_ven
	, cod_ren_entidad_espana
	, cod_ren_centrocont
	, cod_ren_subprodu
	, cod_ren_zona
	, cod_ren_territorial
	, cod_ren_gestorprod
	, cod_ren_gestor
	, id_cto_bdr
	, fc_ren_sdbsfm
	, fc_ren_interes
	, fc_ren_comfin
	, fc_ren_comnofin
	, cod_ren_vincula
	, fc_ren_rof
	, fc_ren_tti
	, cod_ren_mtm
	, cod_ren_nocional
	, cod_ren_divisa_mis
	, flag_ren_moralocal
	, flag_ren_carterizadas
	, ds_ren_nombrecliente
	, cod_per_tipopersona
	, cod_ren_nifcif
	, cod_ren_origen_inf
	, cod_ren_internegocio
	, cod_ren_areanegociocorp
	, cod_productogest
	, cod_segmentogest
	, cod_producto_operacional
	, ds_ren_ficheromis
	, dt_ren_fec_reestruc
	, dt_ren_fec_extrdatos
	, flag_ren_neteo
	, fc_ren_orex
	, fc_ifrs_provision
	, fc_ren_costemensualcto
	, cod_nivel_operacion
	, cod_ren_idru
	, partition_date
FROM bi_corp_staging.rorac_contratos_ap
WHERE perimetro = 'A' 
AND (fc_ren_margenmes != 0 
OR fc_ren_sdbsmv != 0 
OR fc_ren_sfbsmv != 0) ;

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE bi_corp_common.bt_ror_input_pasivo
PARTITION(partition_date) 
	SELECT cod_ren_unidad
	, dt_ren_data
	, cod_ren_finalidaddatos
	, cod_ren_contrato_rorac
	, cod_ren_contrato
	, cod_ren_areanegocio
	, cod_ren_divisa
	, cod_ren_division
	, cod_ren_producto
	, cod_ren_pers_ods
	, fc_ren_margenmes
	, fc_ren_sdbsmv
	, fc_ren_sfbsmv
	, dt_ren_fec_altacto
	, dt_ren_fec_ven
	, cod_ren_entidad_espana
	, cod_ren_centrocont
	, cod_ren_subprodu
	, cod_ren_zona
	, cod_ren_territorial
	, cod_ren_gestorprod
	, cod_ren_gestor
	, id_cto_bdr
	, fc_ren_sdbsfm
	, fc_ren_interes
	, fc_ren_comfin
	, fc_ren_comnofin
	, cod_ren_vincula
	, fc_ren_rof
	, fc_ren_tti
	, cod_ren_mtm
	, cod_ren_nocional
	, cod_ren_divisa_mis
	, flag_ren_moralocal
	, flag_ren_carterizadas
	, ds_ren_nombrecliente
	, cod_per_tipopersona
	, cod_ren_nifcif
	, cod_ren_origen_inf
	, cod_ren_internegocio
	, cod_ren_areanegociocorp
	, cod_productogest
	, cod_segmentogest
	, cod_producto_operacional
	, ds_ren_ficheromis
	, dt_ren_fec_reestruc
	, dt_ren_fec_extrdatos
	, flag_ren_neteo
	, fc_ren_orex
	, fc_ifrs_provision
	, fc_ren_costemensualcto
	, cod_nivel_operacion
	, partition_date
FROM bi_corp_staging.rorac_contratos_ap
WHERE perimetro = 'P' 
AND fc_ren_margenmes != 0 ;

-- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- -- --

-- ${table} debe introducirse la tabla que se desea controlar.
-- ${modelo} debe introducirse el modelo por el que se desea controlar.
-- ${partition_date} debe introducirse la particion por la que se desea controlar.

WITH t1  AS (
                  SELECT partition_date, count(*)  as n,
                  '1' AS p
                  FROM bi_corp_common.stk_rec_gestiones_csm 
                  WHERE partition_date = '2021-06-03'
                   GROUP BY partition_date
                )
SELECT 'particion_vacia' AS id_dqi,
     CASE WHEN ( n = 0 OR
            partition_date IS NULL)
          THEN 1
          ELSE 0
          END AS numerador_dqi,
          1 AS denominador_dqi,
    '2021-06-03' AS fecha_particion,
     'reclamos' AS modelo,
     'bi_corp_common.stk_rec_gestiones_csm' AS tabla
FROM t1 



-- ${table} debe introducirse la tabla que se desea controlar.
-- ${modelo} debe introducirse el modelo por el que se desea controlar.
-- ${partition_date} debe introducirse la particion por la que se desea controlar.


WITH tab AS (SELECT PK,
                     partition_date,
                     count(*) AS cantidad
              FROM ${table}
              WHERE partition_date = ${partition_date} 
              GROUP BY PK,
                       partition_date
               )
SELECT 'duplicado' AS id_dqi,
       sum(CASE WHEN cantidad > 1 
                THEN 1
                ELSE 0
                END) AS numerador_dqi, 
       sum(cantidad) AS denominador_dqi, 
      ${partition_date} AS fecha_particion,
       ${modelo} AS modelo,
       ${table} AS tabla
FROM tab
GROUP BY partition_date



WHERE SUBSTRING(partition_date, 1, 7) = {previous_month_partition_bdr} -- 2021-05






ALTER TABLE bi_corp_common.stk_cys_solicitudcontrato REPLACE COLUMNS (cod_per_nup	bigint ,
	cod_suc_sucursal	int ,
	cod_cys_nrosolicitud	bigint ,
	cod_suc_sucursalcuenta	int ,
	cod_nro_cuenta	bigint ,
	cod_prod_producto	string ,
	cod_prod_subproducto	string ,
	dt_cys_processdate	string ,
	cod_cys_usuarioalta	string ,
	dt_cys_fechalta	string ,
	partition_date string );

SELECT COUNT(1) -- 1.814.071
FROM bi_corp_common.stk_cys_solicitudcontrato
WHERE cod_per_nup IS NOT NULL ; 

