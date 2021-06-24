CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.manual_icaap_tdim
(
         attribute_code	        string,
         dimension_value	    string,
         vision          	    string,
         name	                string,
         second_name	        string,
         father_attribute_code  string,
         father_dimension_value string,
         end_date               string
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/manual/icaap/dim/tdim'