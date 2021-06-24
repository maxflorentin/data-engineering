CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio106_tlog_tabla(
ide_empresa         string,
ide_usuario         string,
ide_transaccion     string,
nom_operacion       string,
nom_producto        string,
nom_req_frontend    string,
cod_estado_trn      string,
tim_ini_trn         string,
tim_fin_trn         string,
mon_transaccion     string,
nro_nup_empresa     string,
nro_nup_individuo	string
)
PARTITIONED BY (
  partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio106/fact/tlog_tabla';
