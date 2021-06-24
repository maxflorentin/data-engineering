CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_pre_cancelacionesprestamos (
cod_pre_entidad string,
cod_suc_sucursal int,
cod_nro_cuenta bigint,
cod_per_nup int,
cod_div_divisa string,
dt_pre_fechaoper string,
dt_pre_fechavalor string,
cod_pre_evento string,
ds_pre_evento string,
fc_pre_capinire decimal(15,2),
fc_pre_intinire decimal(15,2),
fc_pre_cominire decimal(15,2),
fc_pre_gasinire decimal(15,2),
fc_pre_seginire decimal(15,2),
fc_pre_impinire decimal(15,2),
fc_pre_salreal decimal(15,2),
cod_pre_formpago string,
fc_pre_imppago decimal(15,2),
cod_pre_entidadpag string,
cod_pre_sucursalpag int,
cod_pre_cuentapag bigint,
cod_pre_useridumo string,
cod_prod_producto string,
cod_prod_subproducto string,
cod_pre_codides int
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/prestamos/bt_pre_cancelacionesprestamos';