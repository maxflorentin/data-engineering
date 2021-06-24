CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_cue_bajas (
  cod_suc_sucursal INT,
  cod_cue_cuenta BIGINT,
  cod_per_nup INT,
  cod_cue_producto STRING,
  cod_cue_subproducto STRING,
  cod_cue_producto_actual STRING,
  cod_cue_subproducto_actual STRING,
  cod_cue_divisa STRING,
  cod_cue_altair INT,
  flag_cue_bloqueo INT,
  cod_cue_campania STRING,
  ds_cue_campania STRING,
  cod_cue_convenio STRING,
  cod_cue_plan_comision STRING,
  ds_cue_plan_comision STRING,
  fc_cue_saldo_dispuesto DECIMAL(18,2),
  fc_cue_disponible_total_acuerdos DECIMAL(18,2),
  dt_cue_alta STRING,
  cod_cue_cuenta_paquete BIGINT,
  flag_cue_paquete INT,
  cod_cue_producto_paquete INT,
  cod_cue_subproducto_paquete STRING,
  dt_cue_upgrade STRING,
  flag_cue_upgrade_lineal INT,
  flag_cue_marca_contrato_citi INT,
  cod_cue_tarifa STRING,
  ds_cue_tarifa STRING,
  dt_cue_baja STRING,
  cod_cue_canal STRING,
  ds_cue_canal STRING,
  cod_cue_motivo_baja INT
)
PARTITIONED BY (
	  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/cuentas/fact/stk_cue_bajas';