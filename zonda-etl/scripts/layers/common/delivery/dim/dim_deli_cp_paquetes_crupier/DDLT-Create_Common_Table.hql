CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_deli_cp_paquetes_crupier (
               cod_deli_paquete_crupier int
               ,ds_deli_paquete_crupier string
               ,partition_date string

)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/delivery/dim/dim_deli_cp_paquetes_crupier'
;
