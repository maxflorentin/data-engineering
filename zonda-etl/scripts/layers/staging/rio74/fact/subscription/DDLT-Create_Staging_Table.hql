CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio74_subscription(
ID_SUBSCRIPTION     STRING,
ID_ENTITLEMENT      STRING,
ID_SUBSCRIBER       STRING,
ID_DEVICE           STRING,
SEQNUM              STRING,
ID_TIMEFRAME        STRING,
ID_STATUS           STRING,
ID_DAYS_TO_MATURITY STRING,
ID_FREQUENCY        STRING,
MORE_DATA           STRING,
ONE_TIME            STRING,
CREATE_DATE         STRING,
MODIFY_DATE         STRING,
ID_OPERADOR         STRING
)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio74/fact/subscription';