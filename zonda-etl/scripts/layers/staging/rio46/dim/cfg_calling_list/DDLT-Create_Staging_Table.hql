CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio46_cfg_calling_list (
    DBID              STRING,
    TENANT_DBID       STRING,
    NAME              STRING,
    DESCRIPTION       STRING,
    TABL_ACC_DBID     STRING,
    FILTER_DBID       STRING,
    LOG_TABL_ACC_DBID STRING,
    TIME_FROM         STRING,
    TIME_UNTIL        STRING,
    MAX_ATTEMPTS      STRING,
    SCRIPT_DBID       STRING,
    STATE             STRING,
    CSID              STRING,
    TENANT_CSID       STRING,
    TABL_ACC_CSID     STRING,
    FILTER_CSID       STRING,
    LOG_TABL_ACC_CSID STRING,
    SCRIPT_CSID       STRING
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio46/dim/cfg_calling_list'