CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.fm_valores_cuotas_partes
(
fund_code           STRING,
info_date           STRING,
share_value         STRING,
patrimony           STRING
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/fm/fact/valores_cuotas_partes'