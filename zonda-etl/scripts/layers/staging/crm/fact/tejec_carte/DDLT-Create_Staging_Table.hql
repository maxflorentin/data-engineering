CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.crm_tejec_carte
(
USER_ID         STRING,
PESUCADM        STRING,
PEBANCAP        STRING,
PEEJECUE        STRING,
PEJEFARE        STRING,
PESUBSEG        STRING
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/crm/fact/tejec_carte'

