CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio74_pmr_mail(
ID_MAIL             STRING,
ID_REASON           STRING,
DATE_REJECT         STRING,
E_MAIL              STRING,
ID_SIGNATURE        STRING
)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio74/fact/pmr_mail';