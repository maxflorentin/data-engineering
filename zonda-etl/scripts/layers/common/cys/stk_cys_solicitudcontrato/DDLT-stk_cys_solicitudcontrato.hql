CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_cys_solicitudcontrato ( 
	cod_per_nup	bigint ,
	cod_suc_sucursal	int ,
	cod_cys_nrosolicitud	bigint ,
	cod_suc_sucursalcuenta	int ,
	cod_nro_cuenta	bigint ,
	cod_prod_producto	string ,
	cod_prod_subproducto	string ,
	dt_cys_processdate	string ,
	cod_cys_usuarioalta	string ,
	dt_cys_fechalta	string ,
	partition_date string ) 
STORED AS PARQUET 
LOCATION '/santander/bi-corp/common/cys/stk_cys_solicitudcontrato';