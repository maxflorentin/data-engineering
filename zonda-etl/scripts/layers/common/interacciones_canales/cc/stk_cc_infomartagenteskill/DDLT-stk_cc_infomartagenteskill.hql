CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_cc_infomartagenteskill (

cod_cc_legajo string,
ds_cc_nombre string,
ds_cc_apellido string,
ds_cc_codigo_login string,
ds_cc_skill string,
ds_cc_nivel string
)
PARTITIONED BY (
partition_date string)

STORED AS PARQUET
LOCATION
'${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/cc/fact/stk_cc_infomartagenteskill'
