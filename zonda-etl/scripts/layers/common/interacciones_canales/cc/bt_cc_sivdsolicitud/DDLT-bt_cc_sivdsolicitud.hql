
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_cc_sivdsolicitud (

cod_cc_solicitud  string,
cod_cc_circular   string,
cod_cc_promocion  string,
cod_cc_delivery   string,
cod_cc_canal      string,
ds_cc_canal       string,
cod_cc_sucursal   string,
cod_cc_familia    string,
cod_cc_programa   string,
cod_cc_lista      string,
cod_cc_vehiculo   string,
cod_cc_oferta1    string,
cod_cc_oferta2    string,
cod_cc_oferta3    string,
cod_cc_anio       string,
cod_cc_mes        string,
cod_cc_estado     string,
ds_cc_estado      string,
dt_cc_cambioestado  string,
cod_cc_uniqueid   string,
cod_cc_solicitudasol  string,
cod_cc_returncodeasol string,
cod_cc_observacionesasol  string,
cod_cc_nrocuentaasol  string,
cod_cc_legajo  string,
ts_cc_returncodeasol string,
cod_cc_domicilioresumen string,
cod_cc_promocionasol  string,
cod_cc_ultoperacionvisita string,
ds_cc_comentario  string,
cod_cc_campania   string,
cod_cc_prioperacion string,
dt_cc_prioperacion  string,
cod_cc_circuito   string,
ds_cc_circuito    string,
ts_cc_rptcambio   string,
cod_cc_idjournal  string,
cod_cc_oficial    string,
cod_cc_solicitudportal  string,
cod_cc_dependencia  string
)
PARTITIONED BY (
partition_date string)

STORED AS PARQUET
LOCATION
'${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/cc/fact/bt_cc_sivdsolicitud'
