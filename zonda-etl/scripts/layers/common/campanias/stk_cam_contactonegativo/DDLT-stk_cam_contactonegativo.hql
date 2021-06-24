CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_cam_contaconegativo
												 (
cod_per_nup                     bigint,
ds_cam_fuente            		string,
ds_cam_motivo					string,
dt_cam_fecha_alta				string,
dt_cam_fecha_baja				string
)
PARTITIONED BY (
      partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/campanias/fact/stk_cam_contaconegativo'