CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.echeq_actions
(
operation_id  string,
cheque_id     string,
operation     string,
status        string,
target        string,
executed      string,
msg           string,
req           string,
res           string,
cuit          string,
emisor        string,
receptor      string
)
PARTITIONED BY (posted string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/echeq/fact/actions'