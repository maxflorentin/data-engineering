CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio32_histlog_callback_chat(
FECHA                                   STRING,
NUP                                     STRING,
DOCUMENTO                               STRING,
JSESIONID                               STRING,
COD_APP                                 STRING,
DESCRIP                                 STRING,
CANAL                                   STRING,
SEGMENTO_OBP                            STRING,
AGENTE                                  STRING,
ASUNTO                                  STRING,
INTERACTIONID                           STRING
)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio32/fact/histlog_callback_chat';