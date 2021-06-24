CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio147_transaction_log(
FECHA                       STRING,
DOCUMENTO                   STRING,
FECHA_DE_NACIMIENTO         STRING,
CANAL_ACCESO                STRING,
TRANSACCION                 STRING,
COD_ERROR                   STRING,
PID                         STRING,
TIPO                        STRING,
PROCESADO                   STRING,
COMPROBANTE                 STRING,
IMPORTE                     STRING,
MONEDA                      STRING,
F_ALTA_REGISTRO             STRING,
NUP                         STRING,
TIPO_TECLADO                STRING,
IP                          STRING,
NOM_SERVER                  STRING,
NUM_LOG                     STRING,
CUENTA_ORIGEN               STRING,
CUENTA_DESTINO              STRING,
DISPOSITIVO                 STRING
)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio147/fact/transaction_log';