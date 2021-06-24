CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_pre_cuotaspendientes (
cod_pre_entidad string,
cod_suc_sucursal int,
cod_nro_cuenta bigint,
cod_per_nup int,
cod_prod_producto string,
cod_prod_subproducto string,
cod_div_divisa string,
cod_pre_plazo int,
cod_pre_estado string,
cod_pre_recibo int,
dt_pre_fechavencimiento string,
cod_pre_rubrocapital string,
cod_pre_rubrointeres string,
cod_pre_rubrocapajuste string,
cod_pre_rubrointeresdocumentadovencido string,
cod_pre_tipoamortizacion string,
cod_pre_tipotasa string,
fc_pre_saldoteorico decimal(15,2),
fc_pre_importecapital decimal(15,2),
fc_pre_importeinteres decimal(15,2),
fc_pre_ajustecapitalvencido decimal(15,2)
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/prestamos/stk_pre_cuotaspendientes';