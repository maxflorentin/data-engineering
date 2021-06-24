CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_gec_notificacion_canal_cliente (
                cod_gec_notificacion_interno int
                ,cod_gec_canal string
                ,cod_gec_subcanal string
                ,ds_gec_canal string
                ,cod_per_nup int
                ,ds_gec_accion string
                ,ts_gec_estado timestamp
                ,ds_gec_comentarios string
                ,cod_gec_notificacion string
                ,cod_gec_subnotificacion int
                ,cod_gec_grupo int
)
PARTITIONED BY (
      partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/gec/fact/bt_gec_notificacion_canal_cliente'