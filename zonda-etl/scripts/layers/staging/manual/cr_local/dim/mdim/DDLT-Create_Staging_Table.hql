CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.manual_cr_local_mdim
(
         attribute_code	string,
         name	string,
         second_name	string,
         hierarchical_level	string,
         end_date	string
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/manual/cr_local/dim/mdim'