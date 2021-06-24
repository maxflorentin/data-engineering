CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio106_tpcs_log_operacion(
ide_transaccion     string,
ide_solicitud       string,
cod_operacion       string,
cod_estado_sol      string,
mon_importe1        string,
cod_moneda1         string,
can_items1          string,
mon_importe2        string,
cod_moneda2         string,
can_items2			string
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio106/fact/tpcs_log_operacion';
