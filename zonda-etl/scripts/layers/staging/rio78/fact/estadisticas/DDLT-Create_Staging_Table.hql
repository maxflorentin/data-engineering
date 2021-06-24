CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio78_estadisticas(
  id_estadistica string,
  fecha string,
  transaccion string,
  nup_empresa string,
  nup_individuo string,
  importe_debito string,
  importe_credito string,
  moneda string,
  ip string,
  token string,
  comprobante string,
  cuenta_origen string,
  cuenta_destino string,
  nro_solicitud string,
  est_solicitud string,
  fecha_inicio_trn string,
  fecha_fin_trn string,
  cuit_empresa string,
  documento string,
  importe_debito_usd string,
  servidor string,
  cod_res string,
  latitud string,
  longitud string,
  version string,
  email_dest string,
  documento_dest string,
  moneda_debito string,
  cotizacion string,
  empserv string,
  id_terminal string)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio78/fact/estadisticas';