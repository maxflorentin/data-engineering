CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.responsys_opt_out(
event_type_id    	string,
account_id       	string,
list_id          	string,
riid             	string,
customer_id      	string,
event_captured_dt	string,
event_stored_dt  	string,
campaign_id      	string,
launch_id        	string,
email_format     	string,
source           	string,
reason           	string,
email            	string,
contact_info     	string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/responsys/fact/opt_out';