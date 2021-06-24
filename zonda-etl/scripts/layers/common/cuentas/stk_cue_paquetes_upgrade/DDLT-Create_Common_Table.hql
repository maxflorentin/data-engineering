CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_cue_paquetes_upgrade (
  cod_suc_sucursal_paquete INT,
  cod_cue_cuenta BIGINT,
  cod_cue_cuenta_paquete BIGINT,
  cod_cue_producto_paquete STRING,
  cod_cue_subproducto_paquete STRING,
  dt_cue_upgrade STRING,
  cod_cue_grupo_producto INT,
  cod_cue_cuenta_paquete_ant BIGINT,
  cod_cue_producto_paquete_ant STRING,
  cod_cue_subproducto_paquete_ant STRING,
  dt_cue_upgrade_ant STRING,
  cod_cue_grupo_producto_ant INT,
  flag_cue_upgrade_lineal INT
)
PARTITIONED BY (
	  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/cuentas/fact/stk_cue_paquetes_upgrade';