CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.manual_rosetta_segmentation_global
(
         domain_code	    string,
         segmentation_code	string,
         name	            string,
         segmentation_type	string
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/manual/rosetta/dim/segmentation_global'