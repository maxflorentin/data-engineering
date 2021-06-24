CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_deli_stock
(
         cod_deli_courier_shipping int
        ,cod_deli_courier int
        ,cod_deli_courier_tracking string
        ,ts_deli_creacion timestamp
        ,ds_deli_usuario_recibido string
        ,ds_deli_estado string
        ,cod_deli_contrato string
        ,ds_deli_contrato_tipo_servicio string
        ,ds_deli_contrato string
        ,cod_suc_sucursal int
        ,ds_deli_sucursal string
        ,ds_deli_direccion_sucursal string
        ,ds_deli_ciudad_sucursal string
        ,cod_deli_nro_documento string
        ,ds_deli_nombre_apellido string
        ,cod_suc_sucursal_recibido int
        ,dt_deli_ultima_actualizacion string
        ,ds_deli_contenido string
        ,cod_deli_usuario_ultima_actualizacion string
        ,ds_deli_etiqueta string
        ,cod_per_nup string
        ,ts_deli_recibido timestamp
        ,ds_deli_estado_nuevo_envio string
        ,ds_deli_estado_anterior_envio string
        ,dt_deli_creacion_estado string
        ,ds_deli_motivo_estado string
        ,cod_deli_usuario_oficina string
        ,cod_suc_sucursal_derivada int
        ,cod_deli_crupier int
)
PARTITIONED BY (
      partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/delivery/fact/bt_deli_stock';