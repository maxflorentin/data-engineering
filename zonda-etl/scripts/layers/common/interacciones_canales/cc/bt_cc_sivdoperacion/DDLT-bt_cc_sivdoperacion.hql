
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_cc_sivdoperacion (
cod_cc_operacion    string,
cod_cc_contacto     string,
cod_cc_sesion       string,
cod_cc_campania     string,
ds_cc_campania      string,
cod_cc_medio        string,
cod_cc_llamada      string,
cod_cc_legajo       string,
ts_cc_operacion     string,
cod_cc_gestion      string,
ds_cc_gestion       string,
fc_cc_duracion      string,
ts_cc_rellamada     string,
ds_cc_comentarios   string,
flag_cc_unicaoperacion      string,
dt_cc_fecha_visita          string,
ds_cc_horadesde_visita      string,
ds_cc_horahasta_visita      string,
ds_cc_comentario_visita     string,
ds_cc_exportada   string,
ds_cc_vgestor     string,
cod_cc_canal      string,
ds_cc_canal       string,
cod_cc_canalcomunicacion string,
ds_cc_canalcomunicacion string,
cod_cc_sucursal int,
cod_cc_llamada2 string)
PARTITIONED BY (
partition_date string)

STORED AS PARQUET
LOCATION
'${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/cc/fact/bt_cc_sivdoperacion'
