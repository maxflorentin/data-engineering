CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_cjs_bajas (
cod_cjs_suc_caja BIGINT,
cod_cjs_sector_caja BIGINT,
cod_cjs_numero_caja BIGINT,
cod_cjs_tipo_caja BIGINT,
cod_per_nup BIGINT,
dt_cjs_fecha_alta 	STRING,
dt_cjs_fecha_baja STRING,
cod_cjs_suc_contrato BIGINT,
cod_cjs_contrato BIGINT,
dt_cjs_fec_locacion STRING,
cod_marca_baja STRING,
cod_cjs_entidad_debito BIGINT,
cod_cjs_sucursal_debito BIGINT,
cod_cjs_cuenta_debito BIGINT,
cod_cjs_divisa_debito STRING,
cod_cjs_frecuencia BIGINT,
cod_cjs_bonificacion BIGINT,
cod_cjs_deuda_pendiente STRING,
cod_cjs_ind_brio STRING,
dt_cjs_fec_vencimiento STRING,
cod_cjs_forma_pago STRING,
ds_cjs_forma_pago STRING,
cod_cjs_usu_alta STRING,
cod_cjs_usu_ult_act STRING,
dt_cjs_fecha_ult_act STRING,
cod_cjs_ult_periodo_liq STRING,
cod_cjs_campania STRING,
dt_cjs_fec_vto_camp STRING,
dt_cjs_fec_proxima_liq STRING,
dt_cjs_ult_aniomes_desde_liq STRING,
dt_cjs_ult_aniomes_hasta_liq STRING
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/cajas_seguridad/fact/stk_cjs_bajas'