CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_cys_integracionsucursales (
    cod_cys_entidadorigen string,
	cod_suc_sucursalorigen bigint,
	cod_nro_cuentaorigen bigint,
	cod_prod_productoorigen string,
	cod_prod_subproductoorigen string,
	cod_div_divisaorigen string,
	cod_cys_entidaddestino string,
	cod_suc_sucursaldestino bigint,
	cod_nro_cuentadestino bigint,
	cod_prod_productodestino string,
	cod_prod_subproductodestino string,
	cod_div_divisadestino string,
	dt_cys_fechatraspaso string,
	cod_suc_sucuoperacionalorigen int,
	cod_suc_sucoperacionaldestino int,
	cod_per_nup bigint,
	cod_cys_numgrupo string )

PARTITIONED BY (partition_date string)
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/cys/stk_cys_integracionsucursales';