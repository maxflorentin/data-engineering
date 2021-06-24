CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio3_operacionencuesta_rionline(
    operacion  string,
    encuesta string,
    producto string,
    sucursal string,
    nrocuenta string,
    nuptitular string,
    numero_visa string,
    cuenta_visa string,
    numero_amex string,
    cuenta_amex string,
    numero_master string,
    cuenta_master string,
    fecha string
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio3/fact/operacionencuesta_rionline';