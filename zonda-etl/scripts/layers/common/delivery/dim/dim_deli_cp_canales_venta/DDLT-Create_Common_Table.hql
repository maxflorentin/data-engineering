CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_deli_cp_canales_venta (
                cod_deli_canal_venta string
               ,ds_deli_canal_venta string
               ,partition_date string

)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/delivery/dim/dim_deli_cp_canales_venta'
;