CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio147_hb_control_sesion(
NUP                         STRING,
TOKEN                       STRING,
F_ALTA_REGISTRO             STRING,
F_MODIFICACION_REGISTRO     STRING
)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio147/fact/hb_control_sesion';