CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_deli_cp_estados_crupier_comp (
               cod_deli_estado_crupier_comp int
               ,ds_deli_estado_crupier_comp string
               ,partition_date string

)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/delivery/dim/dim_deli_cp_estados_crupier_comp'
;