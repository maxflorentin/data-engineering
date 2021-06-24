CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_gec_canales (
             cod_gec_canal string
            ,cod_gec_subcanal string
            ,ds_gec_canal_nombre string
            ,ds_gec_canal string
            ,flag_gec_activo int
            ,ds_gec_keystore string
            ,partition_date string
)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/gec/dim/dim_gec_canales'
;