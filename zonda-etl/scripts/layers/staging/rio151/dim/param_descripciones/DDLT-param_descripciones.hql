CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio151_param_descripciones ( 
	masche_desc string ,
	masche_cod int ,
	masche_scope string )
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio151/dim/param_descripciones';