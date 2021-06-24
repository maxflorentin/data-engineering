CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio32_reg_elect_datos_hist(
OPERACIONDATOSID                         STRING,
ENTRADA                                 STRING,
SALIDA                                  STRING,
CUSTOM                                  STRING
)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio32/fact/reg_elect_datos_hist';