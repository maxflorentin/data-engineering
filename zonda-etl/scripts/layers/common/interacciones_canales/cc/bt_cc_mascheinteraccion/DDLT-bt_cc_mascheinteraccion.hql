CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_cc_mascheinteraccion (

  cod_cc_interaccion        string,
  cod_per_nup               bigint,
  cod_cc_legajo             string,
  ts_cc_inicio              string,
  ts_cc_cierre              string,
  cod_cc_canalcomunicacion  string,
  ds_cc_canalcomunicacion   string,
  cod_cc_canalventa         string,
  ds_cc_canalventa          string,
  cod_cc_sucursal           string,
  ds_cc_comentario          string,
  cod_cc_motivo             string,
  cod_cc_nrollamado         string,
  cod_cc_origen             string,
  cod_cc_interaccionpadre   string,
  cod_cc_nroticket          string)
PARTITIONED BY (
  partition_date string)
STORED AS PARQUET
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/cc/fact/bt_cc_mascheinteraccion'
