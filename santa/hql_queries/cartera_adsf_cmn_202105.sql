

CREATE TEMPORARY TABLE tmp_cartera AS
SELECT TRIM(id_interface) cod_cys_interface
	, CAST(nup AS BIGINT) cod_per_nup
	, TRIM(entidad) cod_cys_entidad
	, CAST(centro AS INT) cod_suc_sucursal
	, CAST(contrato AS BIGINT) cod_nro_cuenta
	, CAST(producto AS INT) cod_prod_producto
	, TRIM(subprod) cod_prod_subproducto
	, TRIM(divisa) cod_div_divisa
	, TRIM(num_identif) int_cys_identificador
	, TRIM(tip_doc) cod_cys_tipodocumento
	, CAST(num_doc AS BIGINT) per_num_doc
	, SUBSTRING(fec_alta_cto, 1, 10) dt_cys_fechaalta 
	, SUBSTRING(fec_vto_cto, 1, 10) dt_cys_fechavto
	, IF(TRIM(mca_mipyme_cli) = 'null', NULL, TRIM(mca_mipyme_cli)) cod_cys_mipymecli
	, IF(TRIM(mca_mipyme_cto) = 'null', NULL, TRIM(mca_mipyme_cto))  cod_cys_mipymecto
	, TRIM(tipo_cliente) cod_cys_tipocliente
	, TRIM(segmento) cod_cys_segmento
	, IF(TRIM(cod_dest_fondo) = 'null', NULL, TRIM(cod_dest_fondo)) cod_cys_destinofondo
	, TRIM(cod_concepto) cod_cys_concepto
	, TRIM(tipo_gtia) cod_cys_tipogarantia
	, TRIM(cod_gtia) cod_cys_garantia
	, TRIM(tipo_cartera) cod_cys_tipocartera
	, TRIM(familia_rubro) cod_cys_familiarubro
	, TRIM(sit_bcra) cod_cys_situacionbcra
	, TRIM(rubro_altair) cod_cys_rubroaltair
	, TRIM(rubro_bcra) cod_cys_rubrobcra
	, TRIM(rubro_cargabal) cod_cys_rubrocargabal
	, IF(SUBSTRING(fec_prim_inc, 1, 10) = '0001-01-01', NULL, SUBSTRING(fec_prim_inc, 1, 10)) dt_cys_fechapriminc
	, CAST(dias_atraso AS INT) int_cys_diasatraso
	, CAST(tramo AS INT) int_cys_tramo
	, TRIM(imp_deuda) fc_cys_importedeuda
	, TRIM(imp_prevision) fc_cys_importeprovision
	, TRIM(monto_original) fc_cys_montooriginal
	, TRIM(tasa) cod_cys_tasa
	, TRIM(valor_tasa_oper) fc_cys_valortasaoper
	, IF(TRIM(tipo_paquete) = 'null', NULL, TRIM(tipo_paquete)) cod_cys_tipopaquete
	, IF(TRIM(prod_paquete) = 'null', NULL, TRIM(prod_paquete)) cod_cys_prodpaquete
	, IF(TRIM(sprod_paquete) = 'null', NULL, TRIM(sprod_paquete)) cod_cys_sprodpaquete
	, IF(TRIM(prod_original) = 'null', NULL, TRIM(prod_original)) cod_cys_prodoriginal
	, IF(TRIM(sprod_original) = 'null', NULL, TRIM(sprod_original)) cod_cys_sprodoriginal
	, IF(TRIM(SUBSTRING(fec_upgra_paq, 1, 10)) = 'null', NULL, TRIM(SUBSTRING(fec_upgra_paq, 1, 10))) dt_cys_fechaupgradepaq	
	, TRIM(SUBSTRING(fec_alta_prod, 1, 10)) dt_cys_fechaaltaprod
	, TRIM(SUBSTRING(fec_vto_prod, 1, 10)) dt_cys_fechavtoprod	
	, TRIM(marca_altair) cod_cys_marcaaltair
	, TRIM(submarca_altair) cod_cys_submarcaaltair
	, IF(SUBSTRING(fec_ing_moria, 1, 10) = '0001-01-01', NULL, SUBSTRING(fec_ing_moria, 1, 10)) dt_cys_fechaingmoria
	, IF(SUBSTRING(fec_ing_emerix, 1, 10) = 'null', NULL, SUBSTRING(fec_ing_emerix, 1, 10)) dt_cys_fechaingemerix 
	, IF(SUBSTRING(fec_prim_cuota, 1, 10) = 'null', NULL, SUBSTRING(fec_prim_cuota, 1, 10)) dt_cys_fechaprimercuota
	, CAST(plazo AS INT) int_cys_plazo
	, IF(TRIM(cod_proced) = 'null', NULL, TRIM(cod_proced)) cod_cys_proced
	, IF(TRIM(cod_canal) = 'null', NULL, TRIM(cod_canal)) cod_cys_canal
	, IF(TRIM(cod_suc_origen) = 'null', NULL, TRIM(cod_suc_origen)) cod_cys_sucursalorigen
	, TRIM(num_campana) cod_cys_campana
	, CAST(num_shot AS INT) int_cys_numshot
	, TRIM(castigo_contrato) cod_cys_castigocontrato
	, TRIM(castigo_cliente) cod_cys_castigocliente
	, IF(TRIM(entidad_cta) = 'null', NULL, TRIM(entidad_cta)) cod_cys_entidadcta
	, IF(TRIM(centro_cta) = 'null', NULL, CAST(TRIM(centro_cta) AS INT)) cod_suc_sucursalcuenta
	, IF(TRIM(contrato_cta) = 'null', NULL, CAST(TRIM(contrato_cta) AS INT)) cod_nro_cuentacto
	, IF(TRIM(producto_cta) = 'null', NULL, CAST(TRIM(producto_cta) AS INT)) cod_prod_productocta
	, IF(TRIM(subprod_cta) = 'null', NULL, TRIM(subprod_cta)) cod_prod_subproductocta
	, IF(TRIM(divisa_cta) = 'null', NULL, TRIM(divisa_cta)) cod_div_divisacta
	, last_day(to_date(CONCAT(SUBSTRING(periodo,1, 4),'-',SUBSTRING(periodo,5, 2),'-01'))) partition_date 
FROM bi_corp_staging.rio125_adsf_cartera
WHERE partition_date = '2021-02-28' 
AND periodo = '202102' ;


set mapred.job.queue.name=root.dataeng ;
set hive.exec.dynamic.partition.mode=nonstrict ;
INSERT OVERWRITE TABLE bi_corp_common.stk_cys_adsf
PARTITION(partition_date) 
SELECT TRIM(id_interface) cod_cys_interface
	, CAST(nup AS BIGINT) cod_per_nup
	, TRIM(entidad) cod_cys_entidad
	, CAST(centro AS INT) cod_suc_sucursal
	, CAST(contrato AS BIGINT) cod_nro_cuenta
	, CAST(producto AS INT) cod_prod_producto
	, TRIM(subprod) cod_prod_subproducto
	, TRIM(divisa) cod_div_divisa
	, TRIM(num_identif) int_cys_identificador
	, TRIM(tip_doc) cod_cys_tipodocumento
	, CAST(num_doc AS BIGINT) per_num_doc
	, SUBSTRING(fec_alta_cto, 1, 10) dt_cys_fechaalta 
	, SUBSTRING(fec_vto_cto, 1, 10) dt_cys_fechavto
	, IF(TRIM(mca_mipyme_cli) = 'null', NULL, TRIM(mca_mipyme_cli)) cod_cys_mipymecli
	, IF(TRIM(mca_mipyme_cto) = 'null', NULL, TRIM(mca_mipyme_cto))  cod_cys_mipymecto
	, TRIM(tipo_cliente) cod_cys_tipocliente
	, TRIM(segmento) cod_cys_segmento
	, IF(TRIM(cod_dest_fondo) = 'null', NULL, TRIM(cod_dest_fondo)) cod_cys_destinofondo
	, TRIM(cod_concepto) cod_cys_concepto
	, TRIM(tipo_gtia) cod_cys_tipogarantia
	, TRIM(cod_gtia) cod_cys_garantia
	, TRIM(tipo_cartera) cod_cys_tipocartera
	, TRIM(familia_rubro) cod_cys_familiarubro
	, TRIM(sit_bcra) cod_cys_situacionbcra
	, TRIM(rubro_altair) cod_cys_rubroaltair
	, TRIM(rubro_bcra) cod_cys_rubrobcra
	, TRIM(rubro_cargabal) cod_cys_rubrocargabal
	, IF(SUBSTRING(fec_prim_inc, 1, 10) = '0001-01-01', NULL, SUBSTRING(fec_prim_inc, 1, 10)) dt_cys_fechapriminc
	, CAST(dias_atraso AS INT) int_cys_diasatraso
	, CAST(tramo AS INT) int_cys_tramo
	, TRIM(imp_deuda) fc_cys_importedeuda
	, TRIM(imp_prevision) fc_cys_importeprovision
	, TRIM(monto_original) fc_cys_montooriginal
	, TRIM(tasa) cod_cys_tasa
	, TRIM(valor_tasa_oper) fc_cys_valortasaoper
	, IF(TRIM(tipo_paquete) = 'null', NULL, TRIM(tipo_paquete)) cod_cys_tipopaquete
	, IF(TRIM(prod_paquete) = 'null', NULL, TRIM(prod_paquete)) cod_cys_prodpaquete
	, IF(TRIM(sprod_paquete) = 'null', NULL, TRIM(sprod_paquete)) cod_cys_sprodpaquete
	, IF(TRIM(prod_original) = 'null', NULL, TRIM(prod_original)) cod_cys_prodoriginal
	, IF(TRIM(sprod_original) = 'null', NULL, TRIM(sprod_original)) cod_cys_sprodoriginal
	, IF(TRIM(SUBSTRING(fec_upgra_paq, 1, 10)) = 'null', NULL, TRIM(SUBSTRING(fec_upgra_paq, 1, 10))) dt_cys_fechaupgradepaq	
	, TRIM(SUBSTRING(fec_alta_prod, 1, 10)) dt_cys_fechaaltaprod
	, TRIM(SUBSTRING(fec_vto_prod, 1, 10)) dt_cys_fechavtoprod	
	, TRIM(marca_altair) cod_cys_marcaaltair
	, TRIM(submarca_altair) cod_cys_submarcaaltair
	, IF(SUBSTRING(fec_ing_moria, 1, 10) = '0001-01-01', NULL, SUBSTRING(fec_ing_moria, 1, 10)) dt_cys_fechaingmoria
	, IF(SUBSTRING(fec_ing_emerix, 1, 10) = 'null', NULL, SUBSTRING(fec_ing_emerix, 1, 10)) dt_cys_fechaingemerix 
	, IF(SUBSTRING(fec_prim_cuota, 1, 10) = 'null', NULL, SUBSTRING(fec_prim_cuota, 1, 10)) dt_cys_fechaprimercuota
	, CAST(plazo AS INT) int_cys_plazo
	, IF(TRIM(cod_proced) = 'null', NULL, TRIM(cod_proced)) cod_cys_proced
	, IF(TRIM(cod_canal) = 'null', NULL, TRIM(cod_canal)) cod_cys_canal
	, IF(TRIM(cod_suc_origen) = 'null', NULL, TRIM(cod_suc_origen)) cod_cys_sucursalorigen
	, TRIM(num_campana) cod_cys_campana
	, CAST(num_shot AS INT) int_cys_numshot
	, TRIM(castigo_contrato) cod_cys_castigocontrato
	, TRIM(castigo_cliente) cod_cys_castigocliente
	, IF(TRIM(entidad_cta) = 'null', NULL, TRIM(entidad_cta)) cod_cys_entidadcta
	, IF(TRIM(centro_cta) = 'null', NULL, CAST(TRIM(centro_cta) AS INT)) cod_suc_sucursalcuenta
	, IF(TRIM(contrato_cta) = 'null', NULL, CAST(TRIM(contrato_cta) AS INT)) cod_nro_cuentacto
	, IF(TRIM(producto_cta) = 'null', NULL, CAST(TRIM(producto_cta) AS INT)) cod_prod_productocta
	, IF(TRIM(subprod_cta) = 'null', NULL, TRIM(subprod_cta)) cod_prod_subproductocta
	, IF(TRIM(divisa_cta) = 'null', NULL, TRIM(divisa_cta)) cod_div_divisacta
	, partition_date 
FROM bi_corp_staging.rio125_adsf_cartera
WHERE partition_date = '2021-02-28' 
AND periodo = '202102' ;

SELECT * FROM tmp_cartera ;


SHOW PARTITIONS bi_corp_common.stk_cys_adsf ;

DESCRIBE bi_corp_common.stk_cys_adsf ;

-- 173.72 
SELECT * FROM bi_corp_common.stk_cys_adsf WHERE partition_date = '2021-01-31' 
AND cod_nro_cuenta = '800117441439' AND cod_per_nup = '70076860'

-- 468180.78
SELECT * FROM bi_corp_staging.rio125_adsf_cartera
WHERE partition_date = '2021-02-08' AND periodo = '202101'
AND CAST(nup AS BIGINT) = '70076860' AND CAST(contrato AS BIGINT) = '800117441439' 

----------------------------------------------------------------------------------------------------------


cod_centro	num_cuenta	cod_producto  cod_subprodu	cod_divisa


SELECT * FROM santander_business_risk_arda.contratos_deudores_adic 
WHERE data_date_part = '20210129' ;

DESCRIBE santander_business_risk_arda.contratos_deudores_adic ;

-----------------------------------------------------------------------------------------------------------
--ALTER TABLE bi_corp_common.stk_cys_adsf 
--ADD COLUMNS (ds_cys_descripcionsegmento string, ds_cys_categoriaproducto string, ds_cys_detallerenta string, ds_cys_bucket string)
--CASCADE ;

--DESCRIBE bi_corp_common.stk_cys_adsf ;

SHOW PARTITIONS santander_business_risk_arda.contratos_deudores_adic ; -- data_date_part=20210430
SHOW PARTITIONS bi_corp_staging.malpe_ptb_pedt030 ; -- partition_date=2021-04-30
SHOW PARTITIONS bi_corp_staging.risksql5_segmentos ; -- fecha_informacion=2021-04-30


set hive.execution.engine=spark;
set spark.yarn.queue=root.dataeng;
set hive.exec.dynamic.partition.mode=nonstrict;

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
	, TRIM(rda.tip_gtia) cod_cys_tipogarantia
	, TRIM(rda.cod_gtia) cod_cys_garantia
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
	, NULL ds_cys_descripcionsegmento
	, NULL ds_cys_categoriaproducto
	, NULL ds_cys_detallerenta
	, NULL ds_cys_bucket
	, last_day(to_date(CONCAT(SUBSTRING(rda.data_date_part, 1, 4),'-',SUBSTRING(rda.data_date_part, 5, 2),'-01'))) partition_date
FROM santander_business_risk_arda.contratos_deudores_adic rda
WHERE rda.data_date_part = '20210430' )

INSERT OVERWRITE TABLE bi_corp_common.stk_cys_adsf
PARTITION(partition_date) 

SELECT DA.* 
FROM deu_adic DA
LEFT JOIN bi_corp_staging.rio125_adsf_cartera CA --bi_corp_common.stk_cys_adsf CA 
	ON DA.cod_per_nup = CAST(CA.nup AS BIGINT) --CA.cod_per_nup 
	AND DA.cod_nro_cuenta = CAST(CA.contrato AS BIGINT) --CA.cod_nro_cuenta 
	AND DA.cod_prod_producto = TRIM(CA.producto) --CA.cod_prod_producto 
	AND DA.cod_div_divisa = TRIM(CA.divisa) --CA.cod_div_divisa 
	AND DA.partition_date = last_day(to_date(CONCAT(SUBSTRING(CA.periodo,1, 4),'-',SUBSTRING(CA.periodo,5, 2),'-01'))) --CA.partition_date 
	AND CAST(CA.nup AS BIGINT) IS NULL 

UNION ALL 

SELECT TRIM(CA.id_interface) cod_cys_interface
	, CAST(CA.nup AS BIGINT) cod_per_nup
	, TRIM(CA.entidad) cod_cys_entidad
	, CAST(CA.centro AS INT) cod_suc_sucursal
	, CAST(CA.contrato AS BIGINT) cod_nro_cuenta
	, TRIM(CA.producto) cod_prod_producto
	, TRIM(CA.subprod) cod_prod_subproducto
	, TRIM(CA.divisa) cod_div_divisa
	, TRIM(CA.num_identif) int_cys_identificador
	, TRIM(CA.tip_doc) cod_cys_tipodocumento
	, CAST(CA.num_doc AS BIGINT) per_num_doc
	, SUBSTRING(CA.fec_alta_cto, 1, 10) dt_cys_fechaalta 
	, SUBSTRING(CA.fec_vto_cto, 1, 10) dt_cys_fechavto
	, IF(TRIM(CA.mca_mipyme_cli) = 'null', NULL, TRIM(CA.mca_mipyme_cli)) cod_cys_mipymecli
	, IF(TRIM(CA.mca_mipyme_cto) = 'null', NULL, TRIM(CA.mca_mipyme_cto))  cod_cys_mipymecto
	, TRIM(CA.tipo_cliente) cod_cys_tipocliente
	, TRIM(CA.segmento) cod_cys_segmento
	, IF(TRIM(CA.cod_dest_fondo) = 'null', NULL, TRIM(CA.cod_dest_fondo)) cod_cys_destinofondo
	, TRIM(CA.cod_concepto) cod_cys_concepto
	, TRIM(CA.tipo_gtia) cod_cys_tipogarantia
	, TRIM(CA.cod_gtia) cod_cys_garantia
	, TRIM(CA.tipo_cartera) cod_cys_tipocartera
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
	, IF(TRIM(CA.producto_cta) = 'null', NULL, CAST(TRIM(CA.producto_cta) AS INT)) cod_prod_productocta
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
	AND p30.partition_date = '2021-04-30'
LEFT JOIN bi_corp_staging.risksql5_segmentos s 
	ON TRIM(p30.pesegcla) = TRIM(s.ramo) 
	AND s.fecha_informacion = last_day(to_date(CONCAT(SUBSTRING(CA.periodo,1, 4),'-',SUBSTRING(CA.periodo,5, 2),'-01'))) 
LEFT JOIN bi_corp_staging.risksql5_productos p 
	ON TRIM(CA.producto) = TRIM(p.codigo_producto) 
	AND TRIM(CA.subprod) = TRIM(p.codigo_subproducto)  
	AND p.fecha_informacion = last_day(to_date(CONCAT(SUBSTRING(CA.periodo,1, 4),'-',SUBSTRING(CA.periodo,5, 2),'-01')))
WHERE CA.partition_date = '2021-04-30' ; 



-- ################################## VALIDACIONES: 

SELECT COUNT(1) FROM bi_corp_common.stk_cys_adsf WHERE partition_date = '2021-04-30' ; -- 11.082.154 -- 2.612.281


SELECT COUNT(1) FROM bi_corp_staging.rio125_adsf_cartera WHERE partition_date = '2021-04-30' ; -- 11.072.182 -- 11.527.244

SELECT * FROM bi_corp_common.stk_cys_adsf WHERE partition_date = '2021-04-30' AND cod_prod_producto = '07'  ; 


   
 SELECT DISTINCT partition_date
 FROM bi_corp_common.bt_ren_result 
 WHERE cod_ren_vincula = 'null'
	OR cod_ren_gestor = 'null'
  
-- FIX NULL IN BT_REN_RESULT: 
	
-- 2020-08-31
-- 2021-02-28
-- 2019-01-31
-- 2020-09-30
-- 2019-02-28
-- 2017-12-31
-- 2020-01-31
-- 2019-03-31
-- 2019-10-31
-- 2020-02-29
-- 2019-04-30
-- 2019-11-30
-- 2020-03-31
-- 2020-10-31
-- 2019-05-31
-- 2019-12-31
-- 2020-04-30
-- 2020-11-30
-- 2019-06-30
-- 2020-05-31
-- 2020-12-31
-- 2019-07-31
-- 2020-06-30
-- 2019-08-31
-- 2018-12-31
-- 2020-07-31
-- 2021-01-31
-- 2019-09-30

---------------------------------------------------------

   DROP TABLE interaccion_repartition_tmp ;
   CREATE TEMPORARY TABLE interaccion_repartition_tmp AS
   WITH interaccion_repartition AS (
   
	   SELECT cd_interaccion 
	   		, nup 
	   		, cd_ejecutivo 
	   		, dt_inicio 
	   		, dt_cierre 
	   		, cd_canal_comunicacion 
	   		, cd_canal_venta 
	   		, cd_sucursal 
	   		, ds_comentario 
	   		, to_date(dt_inicio) p_date
	   FROM bi_corp_staging.rio151_tbl_interaccion
	   WHERE partition_date = '2000-01-02'
   )
   SELECT * FROM interaccion_repartition ;
   
   
--	 DESCRIBE interaccion_repartition_tmp ;

   
   
SET hive.execution.engine = spark;
SET spark.yarn.queue = root.dataeng;
SET hive.exec.dynamic.partition.mode = nonstrict;
set hive.exec.max.dynamic.partitions=4000;
set hive.exec.max.dynamic.partitions.pernode=4000;

INSERT 	overwrite TABLE bi_corp_common.bt_suc_interaccion PARTITION (partition_date)
SELECT TRIM(TI.cd_interaccion) cod_suc_interaccion
	, CAST(TI.nup AS BIGINT) cod_per_nup
	, TRIM(TI.cd_ejecutivo) cod_suc_legajo
	, to_date(TI.dt_inicio) dt_suc_inicio
	, SUBSTRING(TI.dt_inicio, 1, 19) ts_suc_inicio
	, to_date(TI.dt_cierre) dt_suc_cierre
	, SUBSTRING(TI.dt_cierre, 1, 19) ts_suc_cierre
	, TRIM(TI.cd_canal_comunicacion) cod_suc_canalcomunicacion 
	, TRIM(CC.ds_canal_comunicacion) ds_suc_canalcomunicacion
	, TRIM(TI.cd_canal_venta) cod_suc_canalventa
	, TRIM(CV.ds_canal_venta) ds_suc_canalventa
	, CAST(TI.cd_sucursal AS INT) cod_suc_sucursal
	, TRIM(TI.ds_comentario) ds_suc_comentario
	, TI.p_date partition_date
FROM interaccion_repartition_tmp TI 
LEFT JOIN bi_corp_staging.rio151_tbl_canal_comunicacion CC 
	ON TI.cd_canal_comunicacion = CC.cd_canal_comunicacion 
	AND CC.partition_date = '2021-04-22' 
LEFT JOIN bi_corp_staging.rio151_tbl_canal_venta CV
	ON TI.cd_canal_venta = CV.cd_canal_venta 
	AND CV.partition_date = '2021-04-22' ; 
   

SHOW PARTITIONS bi_corp_common.bt_suc_interaccion ;

SELECT dt_suc_inicio 
	, count(distinct cod_per_nup) clientes
FROM bi_corp_common.bt_suc_interaccion 
WHERE SUBSTRING(dt_suc_inicio, 1, 7) IN ('2020-10','2021-11','2020-12')
GROUP BY dt_suc_inicio ;

-------------------------------------------------------------------------------------

DESCRIBE bi_corp_staging.rio151_tbl_interaccion_producto  ;

set hive.execution.engine = spark;
set spark.yarn.queue = root.dataeng;
set hive.exec.dynamic.partition.mode = nonstrict;

   DROP TABLE IF EXISTS producto_repartition_tmp ;
   CREATE TEMPORARY TABLE producto_repartition_tmp AS
   WITH interaccion_repartition AS (
   
	   SELECT cd_interaccion 
	   		, nup 
	   		, cd_ejecutivo 
	   		, dt_inicio 
	   		, dt_cierre 
	   		, cd_canal_comunicacion 
	   		, cd_canal_venta 
	   		, cd_sucursal 
	   		, ds_comentario 
	   		, to_date(dt_inicio) p_date
	   FROM bi_corp_staging.rio151_tbl_interaccion_producto 
	   WHERE partition_date = '2000-01-01'
   )
   SELECT * FROM producto_repartition ;
  
 





SHOW PARTITIONS bi_corp_common.bt_suc_direccionador

SHOW PARTITIONS bi_corp_staging.nesr_encolador_d



SHOW PARTITIONS bi_corp_staging.malha_alha9600

SHOW PARTITIONS bi_corp_staging.malha_alha9601

SHOW PARTITIONS bi_corp_staging.malha_hals7770

SELECT * FROM bi_corp_staging.malha_alha9600 WHERE partition_date = '2021-04-30' ;

SELECT * FROM bi_corp_staging.malha_alha9601 WHERE partition_date = '2021-04-30' ;

SELECT * FROM bi_corp_staging.malha_hals7770 WHERE partition_date = '2021-04-30' ;





set hive.execution.engine = spark ;
set spark.yarn.queue = root.dataeng ;
set hive.exec.dynamic.partition.mode = nonstrict ;

CREATE TEMPORARY TABLE rubros_contables AS
WITH hals7770 AS (
	
	SELECT TRIM(cuenta_contable_pl1) cod_cys_rubroaltair 
		, SUM(TRIM(REGEXP_REPLACE(saldo_mes, ",", "."))) fc_cys_saldomes
		, SUM(TRIM(REGEXP_REPLACE(saldo_promedio_mes, ",", "."))) fc_cys_saldopromediomes
	FROM bi_corp_staging.malha_hals7770 h7770
	WHERE h7770.partition_date = '2021-04-30'
		GROUP BY TRIM(cuenta_contable_pl1) 

	)

	SELECT TRIM(a9600.entidad) cod_cys_entidad
		, from_unixtime(unix_timestamp(a9600.fecha_informacion ,'dd-mm-yyyy'),'yyyy-mm-dd') dt_cys_fechainformacion
		, TRIM(a9600.rubro_altair) cod_cys_rubroaltair
		, TRIM(a9600.nombre_cuenta) ds_cys_nombrecuenta
		, TRIM(a9600.rubro_bcra) cod_cys_rubrobcra
		, TRIM(a9600.cargabal) cod_cys_cargabal
		, TRIM(a9600.pdn) cod_cys_plandenegocios
		, TRIM(a9600.em) cod_cys_em
		, TRIM(a9600.cuenta_ant) cod_nro_cuentaanterior
		, TRIM(a9600.rubro_niif) cod_cys_rubroniif
		, NULL cod_div_divisa
		, NULL cod_nro_cuentaregularizadora
		-- hals --
		, h7770.fc_cys_saldomes
		, h7770.fc_cys_saldopromediomes
		-- saldos --
		, TRIM(a9600.enero) fc_cys_enero
		, TRIM(a9600.febrero) fc_cys_febrero
		, TRIM(a9600.marzo) fc_cys_marzo
		, TRIM(a9600.abril) fc_cys_abril
		, TRIM(a9600.mayo) fc_cys_mayo
		, TRIM(a9600.junio) fc_cys_junio
		, TRIM(a9600.julio) fc_cys_julio
		, TRIM(a9600.agosto) fc_cys_agosto
		, TRIM(a9600.septiembre) fc_cys_septiembre
		, TRIM(a9600.octubre) fc_cys_octubre
		, TRIM(a9600.noviembre) fc_cys_noviembre
		, TRIM(a9600.diciembre) fc_cys_diciembre
		-- saldo-cierre-anio-anterior --
		, NULL fc_cys_cierreanioanteriorajustinflacion
		-- saldos-ajustados --
		, TRIM(a9601.enero) fc_cys_eneroajustinflacion
		, TRIM(a9601.febrero) fc_cys_febreroajustinflacion
		, TRIM(a9601.marzo) fc_cys_marzoajustinflacion
		, TRIM(a9601.abril) fc_cys_abrilajustinflacion
		, TRIM(a9601.mayo) fc_cys_mayoajustinflacion
		, TRIM(a9601.junio) fc_cys_junioajustinflacion
		, TRIM(a9601.julio) fc_cys_julioajustinflacion
		, TRIM(a9601.agosto) fc_cys_agostoajustinflacion
		, TRIM(a9601.septiembre) fc_cys_septiembreajustinflacion
		, TRIM(a9601.octubre) fc_cys_octubreajustinflacion
		, TRIM(a9601.noviembre) fc_cys_noviembreajustinflacion
		, TRIM(a9601.diciembre) fc_cys_diciembreajustinflacion
		, a9600.partition_date
	FROM bi_corp_staging.malha_alha9600 a9600
	
	LEFT JOIN bi_corp_staging.malha_alha9601 a9601
		ON TRIM(a9600.rubro_altair) = TRIM(a9601.rubro_altair)
		AND a9601.partition_date = '2021-04-30'
	
	LEFT JOIN hals7770 h7770 
		ON TRIM(a9600.rubro_altair) = h7770.cod_cys_rubroaltair
	
	WHERE a9600.partition_date = '2021-04-30' ;

-- DESCRIBE rubros_contables ;

CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_cys_rubroscontables (
	cod_cys_entidad	string ,
	dt_cys_fechainformacion	string ,
	cod_cys_rubroaltair	string ,
	ds_cys_nombrecuenta	string ,
	cod_cys_rubrobcra	string ,
	cod_cys_cargabal	string ,
	cod_cys_plandenegocios	string ,
	cod_cys_em	string ,
	cod_nro_cuentaanterior	string ,
	cod_cys_rubroniif	string ,
	cod_div_divisa	string ,
	cod_nro_cuentaregularizadora	string ,
	fc_cys_saldomes	decimal(20,4) ,
	fc_cys_saldopromediomes	decimal(20,4) ,
	fc_cys_enero	decimal(20,4) ,
	fc_cys_febrero	decimal(20,4) ,
	fc_cys_marzo	decimal(20,4) ,
	fc_cys_abril	decimal(20,4) ,
	fc_cys_mayo	decimal(20,4) ,
	fc_cys_junio	decimal(20,4) ,
	fc_cys_julio	decimal(20,4) ,
	fc_cys_agosto	decimal(20,4) ,
	fc_cys_septiembre	decimal(20,4) ,
	fc_cys_octubre	decimal(20,4) ,
	fc_cys_noviembre	decimal(20,4) ,
	fc_cys_diciembre	decimal(20,4) ,
	fc_cys_cierreanioanteriorajustinflacion	decimal(20,4) ,
	fc_cys_eneroajustinflacion	decimal(20,4) ,
	fc_cys_febreroajustinflacion	decimal(20,4) ,
	fc_cys_marzoajustinflacion	decimal(20,4) ,
	fc_cys_abrilajustinflacion	decimal(20,4) ,
	fc_cys_mayoajustinflacion	decimal(20,4) ,
	fc_cys_junioajustinflacion	decimal(20,4) ,
	fc_cys_julioajustinflacion	decimal(20,4) ,
	fc_cys_agostoajustinflacion	decimal(20,4) ,
	fc_cys_septiembreajustinflacion	decimal(20,4) ,
	fc_cys_octubreajustinflacion	decimal(20,4) ,
	fc_cys_noviembreajustinflacion	decimal(20,4) ,
	fc_cys_diciembreajustinflacion	decimal(20,4)) 
PARTITIONED BY(partition_date	string )
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/santander/bi-corp/common/cys/bt_cys_rubroscontables' ;


set hive.execution.engine = spark ;
set spark.yarn.queue = root.dataeng ;
set hive.exec.dynamic.partition.mode = nonstrict ;

WITH rubros_contables AS (

	SELECT DISTINCT cod_cys_rubroaltair, partition_date FROM (

	SELECT DISTINCT TRIM(a9601.rubro_altair) cod_cys_rubroaltair , partition_date
	FROM bi_corp_staging.malha_alha9601 a9601 
	WHERE a9601.partition_date = '2021-04-30' 
	UNION ALL 
	SELECT DISTINCT TRIM(a9600.rubro_altair) cod_cys_rubroaltair , partition_date
	FROM bi_corp_staging.malha_alha9600 a9600 
	WHERE a9600.partition_date = '2021-04-30' 
	UNION ALL 
	SELECT DISTINCT TRIM(cuenta_contable_pl1) cod_cys_rubroaltair , partition_date
	FROM bi_corp_staging.malha_hals7770 
	WHERE partition_date = '2021-04-30' 
	)A

	)

	, hals7770 AS (
	
	SELECT rc.cod_cys_rubroaltair 
		, SUM(TRIM(REGEXP_REPLACE(h7770.saldo_mes, ",", "."))) fc_cys_saldomes
		, SUM(TRIM(REGEXP_REPLACE(h7770.saldo_promedio_mes, ",", "."))) fc_cys_saldopromediomes
		, rc.partition_date
	FROM rubros_contables rc
	LEFT JOIN bi_corp_staging.malha_hals7770 h7770
	ON rc.cod_cys_rubroaltair = TRIM(h7770.cuenta_contable_pl1)
	AND h7770.partition_date = '2021-04-30'
	GROUP BY rc.cod_cys_rubroaltair, rc.partition_date 

	)
	

INSERT OVERWRITE TABLE bi_corp_common.bt_cys_rubroscontables
PARTITION(partition_date) 

SELECT TRIM(a9600.entidad) cod_cys_entidad
		, from_unixtime(unix_timestamp(a9600.fecha_informacion ,'dd-mm-yyyy'),'yyyy-mm-dd') dt_cys_fechainformacion
		, COALESCE(h7770.cod_cys_rubroaltair, TRIM(a9600.rubro_altair)) cod_cys_rubroaltair
		, TRIM(a9600.nombre_cuenta) ds_cys_nombrecuenta
		, TRIM(a9600.rubro_bcra) cod_cys_rubrobcra
		, TRIM(a9600.cargabal) cod_cys_cargabal
		, TRIM(a9600.pdn) cod_cys_plandenegocios
		, TRIM(a9600.em) cod_cys_em
		, TRIM(a9600.cuenta_ant) cod_nro_cuentaanterior
		, TRIM(a9600.rubro_niif) cod_cys_rubroniif
		, NULL cod_div_divisa
		, NULL cod_nro_cuentaregularizadora
		-- hals --
		, h7770.fc_cys_saldomes
		, h7770.fc_cys_saldopromediomes
		-- saldos --
		, TRIM(REGEXP_REPLACE(a9600.enero, ",", ".")) fc_cys_enero
		, TRIM(REGEXP_REPLACE(a9600.febrero, ",", ".")) fc_cys_febrero
		, TRIM(REGEXP_REPLACE(a9600.marzo, ",", ".")) fc_cys_marzo
		, TRIM(REGEXP_REPLACE(a9600.abril, ",", ".")) fc_cys_abril
		, TRIM(REGEXP_REPLACE(a9600.mayo, ",", ".")) fc_cys_mayo
		, TRIM(REGEXP_REPLACE(a9600.junio, ",", ".")) fc_cys_junio
		, TRIM(REGEXP_REPLACE(a9600.julio, ",", ".")) fc_cys_julio
		, TRIM(REGEXP_REPLACE(a9600.agosto, ",", ".")) fc_cys_agosto
		, TRIM(REGEXP_REPLACE(a9600.septiembre, ",", ".")) fc_cys_septiembre
		, TRIM(REGEXP_REPLACE(a9600.octubre, ",", ".")) fc_cys_octubre
		, TRIM(REGEXP_REPLACE(a9600.noviembre, ",", ".")) fc_cys_noviembre
		, TRIM(REGEXP_REPLACE(a9600.diciembre, ",", ".")) fc_cys_diciembre
		-- saldo-cierre-anio-anterior --
		, NULL fc_cys_cierreanioanteriorajustinflacion
		-- saldos-ajustados --
		, TRIM(REGEXP_REPLACE(a9601.enero, ",", ".")) fc_cys_eneroajustinflacion
		, TRIM(REGEXP_REPLACE(a9601.febrero, ",", ".")) fc_cys_febreroajustinflacion
		, TRIM(REGEXP_REPLACE(a9601.marzo, ",", ".")) fc_cys_marzoajustinflacion
		, TRIM(REGEXP_REPLACE(a9601.abril, ",", ".")) fc_cys_abrilajustinflacion
		, TRIM(REGEXP_REPLACE(a9601.mayo, ",", ".")) fc_cys_mayoajustinflacion
		, TRIM(REGEXP_REPLACE(a9601.junio, ",", ".")) fc_cys_junioajustinflacion
		, TRIM(REGEXP_REPLACE(a9601.julio, ",", ".")) fc_cys_julioajustinflacion
		, TRIM(REGEXP_REPLACE(a9601.agosto, ",", ".")) fc_cys_agostoajustinflacion
		, TRIM(REGEXP_REPLACE(a9601.septiembre, ",", ".")) fc_cys_septiembreajustinflacion
		, TRIM(REGEXP_REPLACE(a9601.octubre, ",", ".")) fc_cys_octubreajustinflacion
		, TRIM(REGEXP_REPLACE(a9601.noviembre, ",", ".")) fc_cys_noviembreajustinflacion
		, TRIM(REGEXP_REPLACE(a9601.diciembre, ",", ".")) fc_cys_diciembreajustinflacion
		, h7770.partition_date
	FROM hals7770 h7770 
	
	LEFT JOIN bi_corp_staging.malha_alha9600 a9600
		ON TRIM(a9600.rubro_altair) = h7770.cod_cys_rubroaltair
		AND a9600.partition_date = '2021-04-30'
	
	LEFT JOIN bi_corp_staging.malha_alha9601 a9601
		ON TRIM(a9601.rubro_altair) = h7770.cod_cys_rubroaltair
		AND a9601.partition_date = '2021-04-30' 
	
	WHERE h7770.partition_date = '2021-04-30' ;


-- 50.110 -- 
SELECT *
FROM bi_corp_common.bt_cys_rubroscontables
WHERE partition_date = '2021-04-30' ;
-- WHERE cod_cys_rubroaltair = '231009' ;



SELECT * FROM bi_corp_common.bt_suc_direccionador 
WHERE partition_date = '2021-04-30' ;



-- 50.110



SELECT DISTINCT TRIM(cuenta_contable_pl1) cod_cys_rubroaltair 
FROM bi_corp_staging.malha_hals7770 
WHERE partition_date = '2021-04-30' 
AND TRIM(cuenta_contable_pl1) NOT IN (
SELECT DISTINCT TRIM(a9600.rubro_altair) cod_cys_rubroaltair
FROM bi_corp_staging.malha_alha9600 a9600 
WHERE a9600.partition_date = '2021-04-30' 
)

SELECT DISTINCT cod_cys_rubroaltair FROM (

	SELECT DISTINCT TRIM(a9601.rubro_altair) cod_cys_rubroaltair
	FROM bi_corp_staging.malha_alha9601 a9601 
	WHERE a9601.partition_date = '2021-04-30' 
	UNION ALL 
	SELECT DISTINCT TRIM(a9601.rubro_altair) cod_cys_rubroaltair
	FROM bi_corp_staging.malha_alha9601 a9601 
	WHERE a9601.partition_date = '2021-04-30' 
	UNION ALL 
	SELECT DISTINCT TRIM(cuenta_contable_pl1) cod_cys_rubroaltair 
	FROM bi_corp_staging.malha_hals7770 
	WHERE partition_date = '2021-04-30' 
)A ;







SHOW PARTITIONS bi_corp_common.bt_suc_movimientos


-- transacciones sellstation movimientos rechazados
014140

SELECT DISTINCT cod_suc_manualauto
FROM bi_corp_common.bt_suc_movimientos
WHERE partition_date='2021-05-12' ;


SELECT * 
FROM bi_corp_common.bt_suc_movimientos 
WHERE partition_date='2021-03-18'
AND cod_suc_canal = 'CAJA' 
AND cod_per_nup = 853798 

SHOW CREATE TABLE bi_corp_staging.rio187_clientes ;





--2021-05-31


SHOW PARTITIONS bi_corp_staging.malpe_ptb_pedt030 ;
SHOW PARTITIONS santander_business_risk_arda.contratos_deudores_adic ;
SHOW PARTITIONS bi_corp_staging.risksql5_productos ;

---------------------------------------------------------------------
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
	, TRIM(s.ds_per_segmento) ds_cys_descripcionsegmento
	, IF(TRIM(s.ds_per_grupo) = 'INDIVIDUOS', TRIM(p.categoria_particulares), TRIM(p.categoria_carterizados)) ds_cys_categoriaproducto	
	, TRIM(s.ds_per_subsegmento) ds_cys_detallerenta
	, NULL ds_cys_bucket
	, '2021-05-31' partition_date
FROM santander_business_risk_arda.contratos_deudores_adic rda
LEFT JOIN bi_corp_staging.malpe_ptb_pedt030 p30 
	ON CAST(rda.num_persona AS BIGINT) = CAST(p30.penumper AS BIGINT)
	AND p30.partition_date = '2021-05-31' -- '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_prevmonth_malpe_pedt030', dag_id='?') }}'
LEFT JOIN bi_corp_common.dim_per_segmentos s 
	ON TRIM(p30.pesegcla) = TRIM(s.cod_per_segmentoduro)
LEFT JOIN bi_corp_staging.risksql5_productos p 
	ON TRIM(rda.cod_producto)  = TRIM(p.codigo_producto) 
	AND TRIM(rda.cod_subprodu) = TRIM(p.codigo_subproducto)  
	AND p.fecha_informacion = '2021-05-31'
WHERE rda.data_date_part = '20210531' ) -- '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_last_working_day_arda', dag_id='?') }}'

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
	, TRIM(s.ds_per_segmento) ds_cys_descripcionsegmento
	, IF(TRIM(s.ds_per_grupo) = 'INDIVIDUOS', TRIM(p.categoria_particulares), TRIM(p.categoria_carterizados)) ds_cys_categoriaproducto	
	, TRIM(s.ds_per_subsegmento) ds_cys_detallerenta
	, CASE WHEN COALESCE(CAST(dias_atraso AS INT),0) = 0 THEN '0' 
		WHEN CAST(dias_atraso AS INT) BETWEEN 1 AND 30 THEN '1-30' 
		WHEN CAST(dias_atraso AS INT) BETWEEN 31 AND 60 THEN '31-60' 
		WHEN CAST(dias_atraso AS INT) BETWEEN 61 AND 90 THEN '61-90' 
		WHEN CAST(dias_atraso AS INT) BETWEEN 91 AND 120 THEN '91-120' 
		WHEN CAST(dias_atraso AS INT) BETWEEN 121 AND 150 THEN '121-150' 
		WHEN CAST(dias_atraso AS INT) BETWEEN 151 AND 180 THEN '151-180' ELSE 'MAS DE 180' END ds_cys_bucket
	, '2021-05-31' partition_date 
FROM bi_corp_staging.rio125_adsf_cartera CA 
LEFT JOIN bi_corp_staging.malpe_ptb_pedt030 p30 
	ON CAST(CA.nup AS BIGINT) = CAST(p30.penumper AS BIGINT)
	AND p30.partition_date = '2021-05-31' -- '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_prevmonth_malpe_pedt030', dag_id='?') }}'
LEFT JOIN bi_corp_common.dim_per_segmentos s 
	ON TRIM(p30.pesegcla) = TRIM(s.cod_per_segmentoduro) 
LEFT JOIN bi_corp_staging.risksql5_productos p 
	ON TRIM(CA.producto) = TRIM(p.codigo_producto) 
	AND TRIM(CA.subprod) = TRIM(p.codigo_subproducto)  
	AND p.fecha_informacion = '2021-05-31'
WHERE CA.partition_date = '2021-05-31' ; -- '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_to', dag_id='?') }}'




SHOW PARTITIONS bi_corp_common.stk_cys_adsf ;

SELECT * FROM bi_corp_common.stk_cys_adsf WHERE partition_date = '2021-04-30' ;


