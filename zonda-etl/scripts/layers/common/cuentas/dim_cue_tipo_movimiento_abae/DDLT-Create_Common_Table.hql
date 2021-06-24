CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_cue_tipo_movimiento_abae (
    cod_cue_tipo_movimiento INT,
    ds_cue_tipo_movimiento STRING
)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/zendesk/dim/dim_cue_tipo_movimiento_abae'
;