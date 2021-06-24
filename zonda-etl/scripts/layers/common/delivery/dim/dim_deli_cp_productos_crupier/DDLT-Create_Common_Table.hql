CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_deli_cp_productos_crupier (
               cod_deli_producto_componente int
               ,ds_deli_producto_componente string
               ,partition_date string

)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/delivery/dim/dim_deli_cp_productos_crupier'
;