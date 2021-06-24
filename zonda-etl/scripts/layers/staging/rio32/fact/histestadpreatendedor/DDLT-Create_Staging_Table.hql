CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio32_histestadpreatendedor(
FECHA                         STRING,
SESION                         STRING,
OPCION_1                         STRING,
OPCION_2                         STRING,
OPCION_3                         STRING,
OPCION_4                         STRING,
OPCION_5                         STRING
)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio32/fact/histestadpreatendedor';