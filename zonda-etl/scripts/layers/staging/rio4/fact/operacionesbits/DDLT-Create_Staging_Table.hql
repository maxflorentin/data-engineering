CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio4_operacionesbits(
NROPERACION 		 STRING,
NRBIT       		 STRING,
VALOR                STRING
)
PARTITIONED BY (FECHAOP string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio4/fact/operacionesbits';