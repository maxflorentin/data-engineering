CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.fm_comisiones
(
person          STRING,
account         STRING,
shares          STRING,
manager_id      STRING,
fund_code       STRING,
patrimony       STRING,
date_process    STRING,
calc_date       STRING,
destination     STRING,
accounting_center STRING,
accounting_account STRING,
percentage      STRING,
commission      STRING
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/fm/fact/comisiones'