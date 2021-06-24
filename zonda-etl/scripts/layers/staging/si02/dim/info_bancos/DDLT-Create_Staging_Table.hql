CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.si02_info_bancos(
CDCENCON STRING,
CDSUCURS STRING,
NUCUENTA STRING,
CDDIVISA STRING,
NUP      STRING
)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/si02/dim/info_bancos';