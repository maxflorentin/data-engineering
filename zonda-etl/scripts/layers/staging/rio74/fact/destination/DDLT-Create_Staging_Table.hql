CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio74_destination(
ID_SUBSCRIBER    STRING,
ID_DEVICE        STRING,
SEQNUM           STRING,
ID_CHANNEL       STRING,
DESTINATION      STRING,
CREATE_DATE      STRING,
MODIFY_DATE      STRING,
ID_STATUS        STRING,
ESTADO           STRING,
REJECT_COUNT     STRING,
REJECT_DATE1     STRING,
REJECT_DATE2     STRING,
SOLUCION_FECHA   STRING,
ID_OPERADOR      STRING,
VALIDADO         STRING,
CHANNEL_BANK     STRING,
SUB_CHANNEL_BANK STRING,
VALIDATE_DATE    STRING
)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio74/fact/destination';