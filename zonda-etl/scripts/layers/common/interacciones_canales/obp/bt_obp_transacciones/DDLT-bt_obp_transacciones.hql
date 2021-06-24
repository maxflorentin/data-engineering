CREATE EXTERNAL TABLE IF NOT EXISTS BI_CORP_COMMON.BT_OBP_TRANSACCIONES (
COD_OBP_TRANSACCION                 STRING,
DS_OBP_TRANSACCION                  STRING,
DS_OBP_TIPO_TECLADO                 STRING,
DS_OBP_TIPO                         STRING,
DS_OBP_PROCESADO                    STRING,
DS_OBP_PID                          STRING,
COD_PER_NUP                         BIGINT,
DS_OBP_NUM_LOG                      STRING,
DS_OBP_NOM_SERVER                   STRING,
COD_OBP_DIVISA                      STRING,
DS_OBP_IP                           STRING,
FC_OBP_IMPORTE                      DECIMAL(19,4),
TS_OBP_FECHA_ALTA                   STRING,
TS_OBP_FECHA                        STRING,
DS_OBP_DOCUMENTO                    BIGINT,
DS_OBP_DISPOSITIVO                  STRING,
COD_OBP_CUENTA_ORIGEN               BIGINT,
COD_OBP_CUENTA_DESTINO              BIGINT,
DS_OBP_COMPROBANTE                  STRING,
DS_OBP_ERROR                        STRING,
COD_OBP_CANAL_ACCESO                STRING
)
PARTITIONED BY (PARTITION_DATE STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/obp/fact/bt_obp_transacciones'