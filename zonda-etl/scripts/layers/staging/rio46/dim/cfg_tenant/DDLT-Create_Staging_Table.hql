CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio46_cfg_tenant (
DBID               STRING,
IS_SERV_PROVIDER   STRING,
NAME               STRING,
PASSWORD           STRING,
ADDRESS_LINE1      STRING,
ADDRESS_LINE2      STRING,
ADDRESS_LINE3      STRING,
ADDRESS_LINE4      STRING,
ADDRESS_LINE5      STRING,
CHARGEABLE_NUMBER  STRING,
TENANT_PERSON_DBID STRING,
PROVID_PERSON_DBID STRING,
IS_SUPER_TENANT    STRING,
STATE              STRING,
CSID               STRING,
TENANT_PERSON_CSID STRING,
PROVID_PERSON_CSID STRING,
CAPACITY_DBID      STRING,
CONTRACT_DBID      STRING,
PARENT_TENANT_DBID STRING
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio46/dim/cfg_tenant'