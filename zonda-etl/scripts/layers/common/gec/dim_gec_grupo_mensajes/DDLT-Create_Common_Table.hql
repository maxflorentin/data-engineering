CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_gec_grupo_mensajes (
             cod_gec_grupo int
            ,cod_gec_mensaje_interno int
            ,cod_gec_canal string
            ,cod_gec_subcanal string
            ,ds_gec_canal string
            ,partition_date string
)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/gec/dim/dim_gec_grupo_mensajes'
;