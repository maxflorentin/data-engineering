CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio46_cfg_group (
DBID             STRING,
GROUP_TYPE       STRING,
TENANT_DBID      STRING,
NAME             STRING,
CAP_TABLE_DBID   STRING,
QUOTA_TABLE_DBID STRING,
STATE            STRING,
DN_GROUP_TYPE    STRING,
CSID             STRING,
TENANT_CSID      STRING,
CAP_TABLE_CSID   STRING,
QUOTA_TABLE_CSID STRING,
CAPACITY_DBID    STRING,
SITE_DBID        STRING,
CONTRACT_DBID    STRING
        )
PARTITIONED BY (
  partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio46/dim/cfg_group'