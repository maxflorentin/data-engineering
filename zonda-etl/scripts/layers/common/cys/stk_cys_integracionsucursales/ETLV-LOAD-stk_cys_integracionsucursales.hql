set mapred.job.queue.name=root.dataeng ;
set hive.exec.dynamic.partition.mode=nonstrict ;

CREATE TEMPORARY TABLE mig_sucursal as
SELECT
	TRIM(entidad_or) cod_cys_entidadorigen,
	cast(centro_or as int) cod_suc_sucursalorigen,
	cast(contrato_or as bigint) cod_nro_cuentaorigen,
	TRIM(producto_or) cod_prod_productoorigen,
	TRIM(subprod_or) cod_prod_subproductoorigen,
	TRIM(divisa_or) cod_div_divisaorigen,
	TRIM(entidad_de) cod_cys_entidaddestino,
	cast(centro_de as int) cod_suc_sucursaldestino,
	cast(contrato_de as bigint) cod_nro_cuentadestino,
	TRIM(producto_de) cod_prod_productodestino,
	TRIM(subprod_de) cod_prod_subproductodestino,
	TRIM(divisa_de) cod_div_divisadestino,
	SUBSTRING(TRIM(fec_traspaso),1,10) dt_cys_fechatraspaso,
	cast(centro_op_or as int) cod_suc_sucuoperacionalorigen,
	cast(centro_op_de as int) cod_suc_sucoperacionaldestino,
	cast(nup as bigint) cod_per_nup,
	TRIM(num_grupo) cod_cys_numgrupo,
	SUBSTRING(TRIM(fec_traspaso),1,10) partition_date
FROM bi_corp_staging.risksql5_rel_contrato_integ_suc
WHERE partition_date = '2020-11-30';

set hive.exec.max.dynamic.partitions=2000;
set hive.exec.max.dynamic.partitions.pernode=2000;
INSERT OVERWRITE TABLE bi_corp_common.stk_cys_integracionsucursales
PARTITION(partition_date)
SELECT * FROM mig_sucursal;