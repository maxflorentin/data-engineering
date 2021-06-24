
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.stk_cc_sivdcontacto (

cod_cc_contacto   string,
cod_cc_tipodoc    string,
ds_per_numdoc     bigint,
cod_cc_expedida   string,
dt_cc_nacio       string,
dt_cc_alta        string,
ds_cc_apellido    string,
ds_cc_nombre      string,
ds_cc_teledisc1   string,
ds_cc_tel1        string,
ds_cc_postal1     string,
ds_cc_teledisc2   string,
ds_cc_tel2        string,
ds_cc_postal2     string,
ds_cc_interno     string,
cod_cc_campania     string,
cod_cc_medio      string,
cod_cc_estado     string,
cod_cc_legajo     string,
ds_cc_clifondos   string,
ds_cc_nivclifon   string,
flag_cc_unicaoperacion  string,
cod_cc_sucursal   int,
cod_per_nup       bigint,
cod_cc_conytipodoc  string,
cod_cc_conynrodoc   string,
ds_cc_capturado   string,
ds_cc_incontactable string,
fc_cc_cantoperac  string,
cod_cc_ultoperacion string,
cod_cc_tipopersona  string,
cod_cc_tipocliente  string,
cod_cc_estado2    string
)
PARTITIONED BY (
partition_date string)

STORED AS PARQUET
LOCATION
'${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/cc/fact/stk_cc_sivdcontacto'
