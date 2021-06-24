CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_cue_comisiones_cobradas  (
 cod_cue_entidad INT,
 cod_suc_sucursal INT,
 cod_cue_cuenta BIGINT,
 cod_cue_divisa STRING,
 cod_cue_div_importe STRING,
 cod_cue_entidad_cargo INT,
 cod_suc_sucursal_cargo INT,
 cod_cue_cuenta_cargo BIGINT,
 cod_cue_div_cargo STRING,
 cod_cue_producto STRING,
 cod_cue_subproducto STRING,
 cod_cue_producto_cargo STRING,
 cod_cue_subprodu_cargo STRING,
 cod_cue_produ_contab STRING,
 cod_cue_subprodu_contab STRING,
 cod_cue_plan STRING,
 cod_cue_concepto STRING,
 cod_cue_comision STRING,
 cod_cue_num_convenio INT,
 ds_cue_operacion STRING,
 ds_cue_zona STRING,
 flag_cue_com_esp INT,
 fc_cue_importe_capitalizado decimal(18,2),
 fc_cue_importe decimal(18,2),
 fc_cue_base_calculo decimal(18,2),
 dec_cue_porc_comision decimal(8,2),
 fc_cue_min_comision decimal(18,2),
 fc_cue_max_comision decimal(18,2),
 dt_cue_fecha_proliq STRING,
 dt_cue_fecha_mov STRING,
 dt_cue_fecha STRING
)
PARTITIONED BY (
      partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/cuentas/fact/bt_cue_comisiones_cobradas'