CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_cys_relacioncuentasciti (
  cod_cys_entidad STRING ,
  cod_suc_sucursal INT ,
  cod_nro_cuenta BIGINT ,
  cod_prod_producto STRING ,
  cod_prod_subproducto STRING ,
  cod_div_divisa STRING ,
  cod_suc_sucursalorigen INT ,
  cod_nro_cuentaorigen BIGINT ,
  cod_cys_entidadorigen STRING )
PARTITIONED BY (partition_date string)
STORED AS PARQUET
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/cys/stk_cys_relacioncuentasciti' ;