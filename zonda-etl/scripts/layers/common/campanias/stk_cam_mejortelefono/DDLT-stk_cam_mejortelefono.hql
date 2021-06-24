CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_cam_mejortelefono (
cod_per_nup 						bigint,
ds_cam_email 						string,
ds_cam_origen 						string,
flag_cam_formato_error				int,
flag_cam_rebote_duro				int,
flag_cam_repetido 					int,
flag_cam_negativo					int,
flag_cam_vigente					int,
ds_cam_prioridad					int,
ds_cam_xmail						string
)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/campanias/fact/stk_cam_mejortelefono'
;
