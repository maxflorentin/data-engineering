CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_rcp_triadkeys(
cod_per_nup bigint,
cod_nro_cuenta bigint,
cod_prod_producto string,
cod_div_divisa string,
cod_suc_sucursal int,
ds_rcp_accountid string,
ds_rcp_codigoestadocacs string,
int_rcp_k467 int,
int_rcp_k468 int,
int_rcp_k540 int,
int_rcp_k542 int,
int_rcp_k544 int,
int_rcp_k552 int,
int_rcp_k555 int,
int_rcp_k590 int,
int_rcp_k593 int,
int_rcp_k596 int,
int_rcp_k597 int,
int_rcp_k598 int,
int_rcp_k599 int,
int_rcp_k600 int,
int_rcp_k601 int,
int_rcp_k602 int,
int_rcp_k603 int,
int_rcp_k604 int,
int_rcp_k605 int,
int_rcp_k839 int)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/recuperaciones/bt_rcp_triadkeys'
;