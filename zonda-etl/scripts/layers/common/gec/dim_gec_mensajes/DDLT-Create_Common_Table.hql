CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_gec_mensajes (
                     cod_gec_notificacion_interno int
                    ,cod_gec_notificacion string
                    ,cod_gec_subnotificacion int
                    ,ds_gec_notificacion string
                    ,ds_gec_titulo string
                    ,ds_gec_url_desktop string
                    ,ds_gec_url_mobile string
                    ,ds_gec_link string
                    ,partition_date string
)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/gec/dim/dim_gec_mensajes'
;