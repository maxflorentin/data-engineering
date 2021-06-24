CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_gec_notificaciones (
         cod_gec_notificacion string
        ,cod_gec_subnotificacion int
        ,ds_gec_notificacion string
        ,ds_gec_titulo string
        ,ds_gec_url_desktop string
        ,ds_gec_url_mobile string
        ,ds_gec_link string
        ,flag_gec_repetible int
        ,flag_gec_inactivable int
        ,flag_gec_agrupable int
        ,partition_date string
)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/gec/dim/dim_gec_notificaciones'
;