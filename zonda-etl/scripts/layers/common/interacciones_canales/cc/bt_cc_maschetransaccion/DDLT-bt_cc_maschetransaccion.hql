CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_cc_maschetransaccion (
  cod_cc_interaccion          string,
  cod_cc_transaccion          string,
  cod_cc_operacion            string,
  ds_cc_datos                 string,
  flag_cc_reservado           int,
  flag_cc_operacionexitosa    int,
  cod_cc_transaccion_reservado string,
  ts_cc_modificaciontransaccion string,
  cod_cc_tipooperacion        string,
  flag_cc_reversabletransaccion int,
  cod_cc_trackeotransaccion   string,
  cod_cc_servicio             string,
  cod_cc_nombre_servicio      string,
  ds_cc_nombre_servicio       string,
  cod_cc_tipo_sevicio          string,
  flag_cc_reversableservicio   int,
  flag_cc_venta               int,
  ts_cc_modificacionservicio  string,
  ds_cc_grupo 			                string)
PARTITIONED BY (PARTITION_DATE STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/cc/fact/bt_cc_maschetransaccion'
