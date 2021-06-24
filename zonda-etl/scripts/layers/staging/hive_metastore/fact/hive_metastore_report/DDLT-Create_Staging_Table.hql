CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.hive_metastore_report(
DB_ID       STRING,
NAME        STRING,
OWNER_NAME  STRING,
TBL_ID      STRING,
TBL_NAME    STRING,
TBL_TYPE    STRING,
COLUMN_NAME STRING,
TYPE_NAME   STRING,
PARAM_KEY   STRING,
PARAM_VALUE STRING
)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/hive_metastore/fact/hive_metastore_report';