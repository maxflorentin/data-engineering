set mapred.job.queue.name=root.dataeng ;
set hive.exec.dynamic.partition.mode=nonstrict ;

WITH deu_adic AS (
SELECT 'D.ADIC' cod_cys_interface
	, CAST(rda.num_persona AS BIGINT) cod_per_nup
	, TRIM(rda.cod_entidad) cod_cys_entidad
	, CAST(rda.cod_centro AS INT) cod_suc_sucursal
	, CAST(rda.num_cuenta AS BIGINT) cod_nro_cuenta
	, CAST(rda.cod_producto AS INT) cod_prod_producto
	, TRIM(rda.cod_subprodu) cod_prod_subproducto
	, TRIM(rda.cod_divisa) cod_div_divisa
	, TRIM(rda.num_identificacion) int_cys_identificador
	, cast(NULL as string) cod_cys_tipodocumento
	, cast(NULL as bigint) per_num_doc
	, cast(NULL as string) dt_cys_fechaalta
	, cast(NULL as string) dt_cys_fechavto
	, cast(NULL as string) cod_cys_mipymecli
	, cast(NULL as string) cod_cys_mipymecto
	, cast(NULL as string) cod_cys_tipocliente
	, cast(NULL as string) cod_cys_segmento
	, cast(NULL as string) cod_cys_destinofondo
	, TRIM(rda.cod_concepto) cod_cys_concepto
	, TRIM(rda.tip_gtia) cod_cys_tipogarantia
	, TRIM(rda.cod_gtia) cod_cys_garantia
	, cast(NULL as string) cod_cys_tipocartera
	, cast(NULL as string) cod_cys_familiarubro
	, cast(NULL as string) cod_cys_situacionbcra
	, TRIM(rda.cod_rubro_altair) cod_cys_rubroaltair
	, TRIM(rda.cod_rubro_bcra) cod_cys_rubrobcra
	, TRIM(rda.cod_rubro_cargabal) cod_cys_rubrocargabal
	, cast(NULL as string) dt_cys_fechapriminc
	, cast(NULL as int) int_cys_diasatraso
	, cast(NULL as int) int_cys_tramo
	, rda.imp_deuda fc_cys_importedeuda
	, rda.imp_prev fc_cys_importeprovision
	, cast(NULL as decimal(20, 4)) fc_cys_montooriginal
	, cast(NULL as string) cod_cys_tasa
	, cast(NULL as string) fc_cys_valortasaoper
	, cast(NULL as string) cod_cys_tipopaquete
	, cast(NULL as string) cod_cys_prodpaquete, cast(NULL as string) cod_cys_sprodpaquete
	, cast(NULL as string) cod_cys_prodoriginal, cast(NULL as string) cod_cys_sprodoriginal
	, cast(NULL as string) dt_cys_fechaupgradepaq, cast(NULL as string) dt_cys_fechaaltaprod
	, cast(NULL as string) dt_cys_fechavtoprod, cast(NULL as string) cod_cys_marcaaltair
	, cast(NULL as string) cod_cys_submarcaaltair, cast(NULL as string) dt_cys_fechaingmoria
	, cast(NULL as string) dt_cys_fechaingemerix, cast(NULL as string) dt_cys_fechaprimercuota
	, cast(NULL as int) int_cys_plazo, cast(NULL as string) cod_cys_proced
	, cast(NULL as string) cod_cys_canal, cast(NULL as string) cod_cys_sucursalorigen
	, cast(NULL as string) cod_cys_campana, cast(NULL as int) int_cys_numshot
	, cast(NULL as string) cod_cys_castigocontrato, cast(NULL as string) cod_cys_castigocliente
	, cast(NULL as string) cod_cys_entidadcta, cast(NULL as int) cod_suc_sucursalcuenta
	, cast(NULL as int) cod_nro_cuentacto, cast(NULL as string) cod_prod_productocta
	, cast(NULL as string) cod_prod_subproductocta, cast(NULL as string) cod_div_divisacta
	, last_day(to_date(CONCAT(SUBSTRING(rda.data_date_part, 1, 4),'-',SUBSTRING(rda.data_date_part, 5, 2),'-01'))) partition_date
FROM santander_business_risk_arda.contratos_deudores_adic rda
WHERE rda.data_date_part = '20210129' )

INSERT OVERWRITE TABLE bi_corp_common.stk_cys_adsf_adic
PARTITION(partition_date) 
SELECT DA.* 
FROM deu_adic DA
LEFT JOIN bi_corp_common.stk_cys_adsf CA 
	ON DA.cod_per_nup = CA.cod_per_nup 
	AND DA.cod_nro_cuenta = CA.cod_nro_cuenta 
	AND DA.cod_prod_producto = CA.cod_prod_producto 
	AND DA.cod_div_divisa = CA.cod_div_divisa 
	AND DA.partition_date = CA.partition_date 
	AND CA.cod_per_nup IS NULL 
UNION ALL 
SELECT * FROM bi_corp_common.stk_cys_adsf
WHERE partition_date = '2021-01-31' ;