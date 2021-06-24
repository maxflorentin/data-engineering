CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio74_tmya_message_log_cd(
MSG_ID          STRING,
NUP             STRING,
IDCLIENTE      STRING,
FECHA           STRING,
ID_ENTITLEMENT  STRING,
DESTINATION     STRING,
ID_ESTADO       STRING,
ID_DEVICE       STRING
)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio74/fact/tmya_message_log_cd';