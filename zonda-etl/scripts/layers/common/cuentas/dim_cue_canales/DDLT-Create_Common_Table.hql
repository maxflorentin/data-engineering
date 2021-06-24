CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_cue_canales (
                cod_cue_canal string
               ,ds_cue_canal string
               ,ds_cue_canal_new string
)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/cuentas/dim/dim_cue_canales'
;