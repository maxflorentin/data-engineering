CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_rcp_accionesemerix(
cod_rcp_nombreaccion string,
cod_rcp_nombrerespuesta string,
dt_rcp_fechaaccion string,
dt_rcp_fecharespuesta string,
cod_per_nup bigint,
ds_rcp_contratolargo string,
cod_rcp_estado string,
ds_rcp_estado string,
cod_rcp_escenario string,
ds_rcp_escenario string,
ds_rcp_usuario string,
cod_rcp_buffete string,
ds_rcp_nombrebuffete string,
ds_rcp_buffete string,
cod_nro_cuenta bigint,
cod_prod_producto string,
cod_prod_subproducto string,
cod_div_divisa string,
cod_suc_sucursal int
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/recuperaciones/stk_rcp_accionesemerix'
;