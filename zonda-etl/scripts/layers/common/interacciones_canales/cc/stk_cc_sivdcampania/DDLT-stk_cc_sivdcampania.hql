CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_cc_sivdcampania (

cod_cc_campania string,
ds_cc_campania string,
flag_cc_activo  string,
flag_cc_agenda  string,
cod_cc_producto string,
cod_cc_appsource  string,
fc_cc_cantcontactos bigint,
ds_cc_prioridad string,
fc_cc_cantoperaciones bigint,
ts_cc_desde     string,
ts_cc_hasta     string,
cod_cc_canal    string,
ts_cc_desdecrm  string,
cod_cc_campbuc  string,
cod_cc_tipocampania string,
fc_cc_cantopagente  bigint,
cod_cc_nroshot  string,
cod_cc_modelocontacto string,
ds_cc_modalidadoutxorigen string,
ts_cc_privigencia string,
cod_cc_legusuariomodif  string,
ts_cc_modificacion  string,
cod_cc_pais     string,
fc_cc_porccontactar bigint,
cod_cc_setgestion string,
cod_cc_listallamado string,
ts_cc_ultvigencia string,
flag_cc_blanquearorigendiscador string
)
PARTITIONED BY (
partition_date string)

STORED AS PARQUET
LOCATION
'${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/cc/fact/stk_cc_sivdcamapnia'
