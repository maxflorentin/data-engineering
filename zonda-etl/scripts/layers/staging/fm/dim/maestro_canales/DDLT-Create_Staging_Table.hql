CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.fm_maestro_canales
(
agent_code      STRING,
code            STRING,
hame            STRING,
type            STRING,
status          STRING,
mark            STRING,
origin          STRING
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/fm/dim/maestro_canales'