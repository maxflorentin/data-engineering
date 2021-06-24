CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_cue_movimientos_genuinos (
  cod_cue_entidad INT,
  cod_suc_sucursal INT,
  cod_cue_cuenta BIGINT,
  cod_cue_divisa STRING,
  cod_cue_producto STRING,
  cod_cue_subproducto STRING,
  dt_cue_primer_movimiento_genuino STRING,
  dt_cue_ultimo_movimiento_genuino STRING
)
PARTITIONED BY (
      partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/cuentas/fact/bt_cue_movimientos_genuinos'