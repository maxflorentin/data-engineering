CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio74_mya_seguimientos(
NUP                STRING,
MSG_ID             STRING,
LINK               STRING,
ENTITLEMENT        STRING,
DESTINATION        STRING,
FECHA_ENVIO        STRING,
FECHA_LOG          STRING,
COND1              STRING,
COND2              STRING,
COND3              STRING,
COND4              STRING
)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio74/fact/mya_seguimientos';