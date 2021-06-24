CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio163_push_notifications(
ID          STRING,
DEEP_LINK   STRING,
ID_MESSAGE  STRING,
MESSAGE     STRING,
NUP         STRING,
CREATED_AT  STRING,
OPENED_AT   STRING,
RECEIVED_AT STRING,
EXPIRY      STRING,
STATE       STRING,
STATUS      STRING,
TITLE       STRING,
URL         STRING,
IMAGE       STRING,
TAG         STRING,
DELETED_AT  STRING
)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio163/fact/push_notifications';