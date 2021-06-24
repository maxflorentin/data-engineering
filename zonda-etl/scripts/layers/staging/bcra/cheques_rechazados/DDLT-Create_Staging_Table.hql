CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.bcra_cheques_rechazados(
        cuit string,
        nro_cheque string,
        fecha_rechazo string,
        monto string,
        causal string,
        fecha_levantamiento string,
        revision string,
        judicial string,
        cuit_relacionado string,
        pago_multa string,
        otro string
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/bcra/bcra_cheques_rechazados';