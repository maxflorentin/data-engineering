CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio74_correspondencia_digital_cheque(
ID_CORRESPONDENCIA          STRING,
NUMERO_CHEQUE               STRING,
IMPORTE                     STRING

)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio74/fact/correspondencia_digital_cheque';