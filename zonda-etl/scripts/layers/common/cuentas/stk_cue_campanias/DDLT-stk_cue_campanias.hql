CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_cue_campanias(
    cod_cue_entidad STRING,
    cod_cue_cuenta BIGINT,
    cod_suc_sucursal INT,
    dt_cue_fecha_desde STRING,
    dt_cue_fecha_hasta STRING,
    cod_cue_campania STRING,
    cod_cue_divisa STRING,
    cod_cue_plan STRING,
    cod_cue_producto STRING,
    cod_cue_subproducto STRING,
    int_cue_plazo INT
)
PARTITIONED BY (
	  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/cuentas/fact/stk_cue_campanias';