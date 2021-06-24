CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_adm_ind_productopromocion (
cod_adm_producto int,
ds_adm_producto string,
ds_adm_productodetalle string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/admision/dim_adm_ind_productopromocion'
;