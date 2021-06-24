CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.malbgc_acreditaciones(
311_ENT_EMPLEADO        STRING,
311_SUC_EMPLEADO        STRING,
311_NRO_EMPLEADO        STRING,
311_DIV_EMPLEADO        STRING,
311_ENT_EMPLEADOR       STRING,
311_SUC_EMPLEADOR       STRING,
311_NRO_EMPLEADOR       STRING,
311_DIV_EMPLEADOR       STRING,
311_CUIT_EMPLEADOR      STRING,
311_SIGNO_IMPORTE       STRING,
311_IMPORTE             STRING,
311_PROGRAMA            STRING,
311_SISTEMA             STRING,
311_COD_CONVENIO        STRING,
311_FECHA_INFO          STRING,
311_CBU_EMPLEADO        STRING,
311_CUIL_EMPLEADO       STRING,
311_TIPO_CUENTA         STRING,
311_TIPO_PROC           STRING,
311_DENOMINACION        STRING,
311_FECHA_ACRED         STRING,
311_SUSCRIPTOR          STRING,
311_NUMPER              STRING,
311_EMPRESA_PIRYP       STRING
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/malbgc/fact/acreditaciones'