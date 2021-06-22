
--  2021-05-31 -- ok 
--	2021-04-30 -- ok
--	2021-03-31 -- ok
--	2021-02-26 -- ok
--	2021-01-29
--	2020-12-23
--	2020-11-30
--	2020-10-30
--	2020-09-30
--	2020-08-31
--	2020-07-31
--	2020-06-30
--	2020-05-29
--	2020-04-30
--	2020-03-30
--	2020-02-28
--	2020-01-31
--	2019-12-27
--	2019-11-29
--	2019-10-31
--	2019-09-30
--	2019-08-30
--	2019-07-31
--	2019-06-28
--	2019-05-31
--	2019-04-30
--	2019-03-29
--	2019-02-28
--	2019-01-31
--	2018-12-28
--	2018-11-29
--	2018-10-31
--	2018-09-28
--	2018-08-31
--	2018-07-31
--	2018-06-29
--	2018-05-31
--	2018-04-27
--	2018-03-28
--	2018-02-28
--	2018-01-31




SHOW PARTITIONS bi_corp_staging.malpe_ptb_pedt030 ;
SHOW PARTITIONS santander_business_risk_arda.contratos_deudores_adic ;
SHOW PARTITIONS bi_corp_staging.risksql5_productos ;
SHOW PARTITIONS bi_corp_common.stk_cys_adsf ;
SHOW PARTITIONS bi_corp_staging.rio125_adsf_cartera
-- 2021-02-08 -- partition_hist_completa

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
	, '2021-02-26' partition_date
FROM santander_business_risk_arda.contratos_deudores_adic rda
LEFT JOIN bi_corp_staging.malpe_ptb_pedt030 p30 
	ON CAST(rda.num_persona AS BIGINT) = CAST(p30.penumper AS BIGINT)
	AND p30.partition_date = '2021-02-26' -- '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_prevmonth_malpe_pedt030', dag_id='?') }}'
LEFT JOIN bi_corp_common.dim_per_segmentos s 
	ON TRIM(p30.pesegcla) = TRIM(s.cod_per_segmentoduro)
LEFT JOIN bi_corp_staging.risksql5_productos p 
	ON TRIM(rda.cod_producto)  = TRIM(p.codigo_producto) 
	AND TRIM(rda.cod_subprodu) = TRIM(p.codigo_subproducto)  
	AND p.fecha_informacion = '2021-02-26'
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
	, '2021-02-26' partition_date 
FROM bi_corp_staging.rio125_adsf_cartera CA 
LEFT JOIN bi_corp_staging.malpe_ptb_pedt030 p30 
	ON CAST(CA.nup AS BIGINT) = CAST(p30.penumper AS BIGINT)
	AND p30.partition_date = '2021-02-26' -- '{{ ti.xcom_pull(task_ids='InputConfig', key='max_partition_prevmonth_malpe_pedt030', dag_id='?') }}'
LEFT JOIN bi_corp_common.dim_per_segmentos s 
	ON TRIM(p30.pesegcla) = TRIM(s.cod_per_segmentoduro) 
LEFT JOIN bi_corp_staging.risksql5_productos p 
	ON TRIM(CA.producto) = TRIM(p.codigo_producto) 
	AND TRIM(CA.subprod) = TRIM(p.codigo_subproducto)  
	AND p.fecha_informacion = '2021-02-26'
WHERE CA.partition_date = '2021-02-26' ; -- '{{ ti.xcom_pull(task_ids='InputConfig', key='previous_month_to', dag_id='?') }}'



-- -- -- -- -- -- -- -- -- -- 



select cod_suc_canal, cod_suc_sector, cod_per_nup, cod_suc_maquina, ts_suc_horatransaccion, cod_per_tipopersona, cod_suc_sucursalcuenta, cod_suc_transaccion, ds_suc_transaccion, cod_suc_usuario, cod_per_cuadrante, ds_per_subsegmento, cod_div_divisa, count(*) as cantidad 
from bi_corp_common.bt_suc_movimientos
where partition_date = '2021-06-08' and cod_suc_canal = 'CAJA'
group by cod_suc_canal, cod_suc_sector, cod_per_nup, cod_suc_maquina, ts_suc_horatransaccion, cod_per_tipopersona, cod_suc_sucursalcuenta, cod_suc_transaccion, ds_suc_transaccion, cod_suc_usuario, cod_per_cuadrante, ds_per_subsegmento, cod_div_divisa
having count(*) > 1
order by cantidad desc


SHOW PARTITIONS bi_corp_common.bt_suc_movimientos




WITH recaud_oper AS (

	SELECT ide_pgo
  		, ind_forma_pago 
  		, IF(TRIM(ind_forma_pago) = 'E', COUNT(1) OVER (PARTITION BY ide_pgo), 0) cantidad_efectivo
  		, IF(TRIM(ind_forma_pago) = 'E', SUM(imp_mov) OVER (PARTITION BY ide_pgo), 0) importe_efectivo
  		, IF(TRIM(ind_forma_pago) = 'C', COUNT(1) OVER (PARTITION BY ide_pgo), 0) cantidad_cheque
  		, IF(TRIM(ind_forma_pago) = 'C', SUM(imp_mov) OVER (PARTITION BY ide_pgo), 0) importe_cheque
  		, IF(TRIM(ind_forma_pago) NOT IN ('E','C'), COUNT(1) OVER (PARTITION BY ide_pgo), 0) cantidad_otros
  		, IF(TRIM(ind_forma_pago) NOT IN ('E','C'), SUM(imp_mov) OVER (PARTITION BY ide_pgo), 0) importe_otros
	FROM bi_corp_staging.rio61_test_inter_recaud_oper 
	WHERE partition_date = '2021-06-08'

	)
	
SELECT * FROM recaud_oper WHERE importe_cheque > 0 ;








-- -- -- -- -- -- -- -- -- -- 

-- staging adsf




























