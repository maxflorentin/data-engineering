CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.rosetta_rnkd(
	legal_entity_code    string,
	native_key           string,
	segmentation_code    string,
	attribute_code       string,
	local_value          string,
	global_value         string,
	segmentation_type    string,
	end_date             string,
	partition_date       string
)
PARTITIONED BY (
            domain_code string)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/santander/bi-corp/common/rosetta/fact/rnkd/rosetta_rnkd'