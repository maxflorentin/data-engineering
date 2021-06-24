CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_cc_maschetrackeo (

    cod_cc_interaccion  string,
    cod_cc_evento       string,
    ds_cc_envento       string,
    ts_cc_evento        string,
    ds_cc_infoadicional string,
    cod_cc_nrollamado   string,
    cod_cc_transaccion  string)
  PARTITIONED BY (
    partition_date string)
  STORED AS PARQUET
  LOCATION
    '${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/cc/fact/bt_cc_maschetrackeo'
