CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_seg_tipo_seguro (
cod_seg_ramo bigint,
ds_seg_ramo string,
cod_seg_producto string,
ds_seg_producto string,
cod_seg_plan string,
ds_seg_plan string,
cod_seg_tipo_seguro string,
ds_seg_tipo_seguro string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/seguros/dim/dim_seg_tipo_seguro'