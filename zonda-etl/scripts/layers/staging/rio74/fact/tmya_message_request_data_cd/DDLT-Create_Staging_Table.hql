CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio74_tmya_message_request_data_cd(
MSG_REQ_ID                     STRING,
ID_ENTITLEMENT                 STRING,
CREATE_TIMESTAMP               STRING,
MANUAL                         STRING,
PROCESSED                      STRING,
DATA                           STRING,
TIMESTAMP_INICIO_PROCESAMIENTO STRING
)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio74/fact/tmya_message_request_data_cd';