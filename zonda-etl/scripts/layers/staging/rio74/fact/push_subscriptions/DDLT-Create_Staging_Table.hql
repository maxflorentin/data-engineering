CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio74_push_subscriptions(
ID            STRING,
ACTIVE        STRING,
APP_VERSION   STRING,
DEVICE_ID     STRING,
MANUFACTURER  STRING,
MODEL         STRING,
MODEL_CODE    STRING,
NUP           STRING,
OS_VERSION    STRING,
PLATFORM      STRING,
REGISTERED_AT STRING,
LAST_REGISTER STRING,
TOKEN         STRING
)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio74/fact/push_subscriptions';