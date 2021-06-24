CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_cc_sivdcircuito (

cod_cc_circuito string,
ds_cc_circuito string,
fc_cc_diasbackoffice bigint,
flag_cc_activo int,
cod_cc_tipoentrevista string,
cod_cc_oca string,
cod_cc_pais string,
cod_cc_tiposolicitud string,
ds_cc_procesotibco string,
cod_cc_tipocircuito string,
ds_cc_tipocircuito string,
flag_cc_reversa int
)
PARTITIONED BY (
partition_date string)

STORED AS PARQUET
LOCATION
'${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/cc/dim/dim_cc_sivdcircuito'
