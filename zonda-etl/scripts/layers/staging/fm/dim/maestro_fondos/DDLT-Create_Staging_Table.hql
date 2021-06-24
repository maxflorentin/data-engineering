CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.fm_maestro_fondos
(
manager_id          STRING,
manager_fund_code   STRING,
manager_fund_class  STRING,
fund_code           STRING,
name                STRING,
currency            STRING,
isin                STRING,
fund_cnv_code       STRING,
fund_custody_code   STRING,
format              STRING,
lack_days           STRING,
fund_type_id        STRING,
fact_sheet_url      STRING,
regulation_url      STRING,
status_id           STRING,
is_enabled          STRING
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/fm/dim/maestro_fondos'