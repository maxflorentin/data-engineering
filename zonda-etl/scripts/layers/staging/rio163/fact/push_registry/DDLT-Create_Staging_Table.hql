CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio163_push_registry(
ID                       STRING,
OPERATION_DATE           STRING,
DEVICE_ID                STRING,
NOTIFICATION_ID          STRING,
STATE                    STRING
)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio163/fact/push_registry';