CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_rcp_pagosmoria (
cod_suc_sucursal int,
cod_nro_cuenta bigint,
cod_prod_producto string,
cod_prod_subproducto string,
cod_div_divisa string,
dt_rcp_fechacanceoperacion string,
cod_rcp_cancelacion string,
cod_rcp_numrecibo int,
fc_rcp_cont_imptotcanco decimal(17, 4),
fc_rcp_cont_imptotdevg decimal(17, 4),
fc_rcp_nocont_imptotndevg decimal(17, 4),
fc_rcp_cont_impdevcapital decimal(17, 4),
fc_rcp_cont_impdevinteres decimal(17, 4),
fc_rcp_cont_impdevajuste1 decimal(17, 4),
fc_rcp_cont_impudevtotal decimal(17, 4),
fc_rcp_cont_impudeviva1 decimal(17, 4),
fc_rcp_cont_impudeviva2 decimal(17, 4),
fc_rcp_cont_impudevingb decimal(17, 4),
fc_rcp_cont_impudevimpe decimal(17, 4),
fc_rcp_cont_impudevotro decimal(17, 4),
fc_rcp_nocont_imperccomision decimal(17, 4),
fc_rcp_nocont_impercsegvida decimal(17, 4),
fc_rcp_nocont_impercsegince decimal(17, 4),
fc_rcp_nocont_impercsegtotal decimal(17, 4),
fc_rcp_nocont_impercgastosmdug decimal(17, 4),
fc_rcp_nocont_impercintcps decimal(17, 4),
fc_rcp_nocont_impercintmor decimal(17, 4),
fc_rcp_nocont_impercajuste decimal(17, 4),
fc_rcp_nocont_impercinteres decimal(17, 4),
fc_rcp_nocont_impuperctotal decimal(17, 4),
fc_rcp_nocont_impuperciva1 decimal(17, 4),
fc_rcp_nocont_impuperciva2 decimal(17, 4),
fc_rcp_nocont_impupercingb decimal(17, 4),
fc_rcp_nocont_impupercinge decimal(17, 4),
fc_rcp_nocont_impupercotro decimal(17, 4)
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/recuperaciones/stk_rcp_pagosmoria'
;