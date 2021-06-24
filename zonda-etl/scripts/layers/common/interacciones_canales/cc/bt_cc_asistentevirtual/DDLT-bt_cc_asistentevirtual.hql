
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_cc_asistentevirtual (

cod_per_nup string,
cod_cc_genesys string,
ds_cc_canal string,
cod_cc_sesion string,
ts_cc_chat string,
ds_cc_emisortipo string,
ds_cc_emisornombre string,
ds_cc_mensaje string,
ds_cc_intencion string,
ds_cc_confidencia string,
flag_cc_utilpreguntarespuesta string,
ds_cc_preguntaposible string,
ds_cc_temasugerncia string,
ds_cc_opciones string,
flag_cc_transferidoahumano string,
flag_cc_fueposiblepregunta string,
flag_cc_fueposiblesugenrencia string,
flag_cc_fueopciones string
)
PARTITIONED BY (
partition_date string)

STORED AS PARQUET
LOCATION
'${DATA_LAKE_SERVER}/santander/bi-corp/common/interaciones_canales/cc/fact/bt_cc_asistentevirtual'
