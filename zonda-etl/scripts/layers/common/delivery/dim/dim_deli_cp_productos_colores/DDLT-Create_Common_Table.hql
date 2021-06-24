CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_deli_cp_productos_colores (
               cod_deli_color_producto_tarjeta int
               ,ds_deli_color_producto_tarjeta string
               ,partition_date string
)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/delivery/dim/dim_deli_cp_productos_colores'
;