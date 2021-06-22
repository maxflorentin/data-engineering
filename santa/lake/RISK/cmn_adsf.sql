-- ##################################

set hive.exec.max.dynamic.partitions=2000;
set hive.exec.max.dynamic.partitions.pernode=2000;
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
	, IF(TRIM(num_campana) = 'null', NULL, TRIM(num_campana)) cod_cys_campana
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
WHERE partition_date = '2021-02-28' --'2021-02-08' 
--	AND periodo = '202101' ;


SHOW PARTITIONS bi_corp_staging.rio125_adsf_cartera