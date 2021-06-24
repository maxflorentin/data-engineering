CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_pre_reestructuracioncontratos (
cod_pre_entidad string,
cod_suc_sucursal int,
cod_nro_cuenta bigint,
cod_per_nup int,
cod_prod_producto string,
cod_prod_subproducto string,
cod_div_divisa string,
cod_suc_sucursalreestructuracion string,
cod_pre_contratoreestructuracion string,
cod_pre_productoreestructuracion string,
cod_pre_subproductoreestructuracion string,
cod_pre_divisareestructuracion string,
dt_pre_fecrefinanc string,
cod_pre_reesctr string,
flag_pre_indantconint int,
cod_pre_marcasegesp string,
cod_pre_marcagarra string,
cod_pre_submarcagarra string,
dt_pre_fecprimerimp string,
fc_pre_importerefinanciado decimal(13,4),
fc_pre_importerefinanciadodolpesif decimal(13,4),
fc_pre_importeintereses decimal(11,2)
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/prestamos/bt_pre_reestructuracioncontratos';