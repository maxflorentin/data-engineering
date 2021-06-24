CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_cam_blacklist_seguros
												 (
cod_cam_tipodoc            		string,
ds_cam_nrodoc                   bigint,
ds_cam_nombre					string,
cod_per_nup                     bigint
)
PARTITIONED BY (
      partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/campanias/fact/stk_cam_blacklist_seguros'