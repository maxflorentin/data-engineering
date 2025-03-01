CREATE EXTERNAL TABLE bi_corp_common.stk_cue_acuerdos_especiales (
  cod_suc_sucursal INT,
  cod_cue_cuenta BIGINT,
  cod_cue_divisa STRING,
  cod_cue_secuencia INT,
  cod_cue_tipo_acuerdo STRING,
  cod_cue_estado STRING,
  fc_cue_limite DECIMAL(18,2),
  fc_cue_limite_renovacion DECIMAL(18,2),
  cod_cue_period_liquidacion STRING,
  cod_cue_tipo_liquidacion STRING,
  cod_cue_clase_liquidacion STRING,
  cod_cue_dia_liquidacion INT,
  cod_cue_prioridad INT,
  cod_cue_tarifa STRING,
  flag_cue_clase_taf INT,
  fc_cue_tipo_interes DECIMAL(18,5),
  fc_cue_coeficiente DECIMAL(18,2),
  fc_cue_puntos INT,
  cod_cue_tipoint_inc_sbrg INT,
  cod_cue_tasa_max_conv STRING,
  fc_cue_importe_cambio DECIMAL(18,2),
  fc_cue_limite_original DECIMAL(18,2),
  fc_cue_intereses_devengados_deuda DECIMAL(18,2),
  cod_cue_refer_liq INT,
  flag_cue_renauto INT,
  flag_cue_ind_intv_sal_dia INT,
  flag_cue_ind_pet_liq INT,
  cod_cue_tipo_vencimiento STRING,
  cod_cue_num_vencimiento INT,
  fc_cue_dias_uso INT,
  fc_cue_dias_desp_pro INT,
  dt_cue_ultren STRING,
  dt_cue_inicio STRING,
  dt_cue_vcto STRING,
  dt_cue_ultima_liquidacion STRING)
PARTITIONED BY (
      partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/cuentas/fact/stk_cue_acuerdos_especiales'
