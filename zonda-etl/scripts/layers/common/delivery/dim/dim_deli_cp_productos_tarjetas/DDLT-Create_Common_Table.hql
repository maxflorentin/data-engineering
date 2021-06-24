CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_deli_cp_productos_tarjetas (
			   cod_deli_prod_tarjeta int
               ,cod_deli_subproducto_tarjeta string
               ,ds_deli_producto_tarjeta string
               ,cod_deli_origen_producto_tarjeta int
               ,cod_deli_color_producto_tarjeta int
               ,partition_date string

)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/delivery/dim/dim_deli_cp_productos_tarjetas'
;
