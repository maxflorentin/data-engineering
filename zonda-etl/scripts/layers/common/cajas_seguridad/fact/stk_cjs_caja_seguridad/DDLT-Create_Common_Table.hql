CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_cjs_caja_seguridad(
cod_cjs_suc_caja BIGINT,
cod_cjs_sector_caja BIGINT,
cod_cjs_numero_caja BIGINT,
cod_cjs_tipo_caja BIGINT,
cod_cjs_ind_ocupacion STRING,
dt_cjs_fecha_alta 	STRING,
cod_cjs_suc_contrato BIGINT,
cod_cjs_contrato BIGINT,
dt_cjs_fec_locacion STRING,
dt_cjs_fec_vencimiento STRING,
cod_cjs_forma_pago STRING,
ds_cjs_forma_pago STRING,
cod_cjs_entidad_debito BIGINT,
cod_cjs_sucursal_debito BIGINT,
cod_cjs_cuenta_debito BIGINT,
cod_cjs_divisa_debito STRING,
cod_cjs_frecuencia BIGINT,
cod_cjs_bonificacion BIGINT,
cod_cjs_deuda_pendiente STRING,
dt_cjs_fec_alta_contrato STRING,
cod_cjs_ind_brio STRING,
cod_cjs_grupo_caja STRING,
ds_cjs_grupo_caja STRING,
cod_cjs_medida_caja STRING,
cod_per_nup BIGINT,
cod_per_tipo_persona	STRING,
cod_cjs_comision BIGINT,
ds_cjs_comision STRING,
cod_cjs_zona	BIGINT,
dt_cjs_fec_vig_desde STRING,
dt_cjs_fec_vig_hasta STRING,
cod_cjs_campania STRING,
fc_cjs_imp_comision DECIMAL(13,2)
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/cajas_seguridad/fact/stk_cjs_caja_seguridad'