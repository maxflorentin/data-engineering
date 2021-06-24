CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.si02_swfmsgbody(
QUEUE      	STRING,
SEQ        	STRING,
LINE_ORDER 	STRING,
FIELD      	STRING,
SUB_FIELD  	STRING,
SUB_LINE   	STRING,
TXCOMMENT  	STRING,
TEXT_VAL   	STRING,
NUMBER_VAL 	STRING,
DATE_VAL   	STRING
)
PARTITIONED BY (SWIFT_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/si02/fact/swfmsgbody';