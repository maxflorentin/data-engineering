CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_cjs_comisiones (
cod_cjs_suc_contrato BIGINT,
cod_cjs_contrato BIGINT,
cod_cjs_comision BIGINT,
ds_cjs_comision STRING,
cod_cjs_periodo_liq STRING,
dt_cjs_fecha_liq STRING,
cod_cjs_concepto STRING,
fc_cjs_imp_concepto DECIMAL(13,2),
fc_cjs_imp_impuesto DECIMAL(13,2),
cod_cjs_forma_pago STRING,
ds_cjs_forma_pago STRING,
cod_cjs_entidad_debito BIGINT,
cod_cjs_sucursal_debito BIGINT,
cod_cjs_cuenta_debito BIGINT,
cod_cjs_divisa_debito STRING,
dt_cjs_fecha_pago STRING,
cod_cjs_canal STRING,
ds_cjs_canal STRING,
cod_per_nup BIGINT,
cod_cjs_usuario_oper STRING
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/cajas_seguridad/fact/bt_cjs_comisiones'