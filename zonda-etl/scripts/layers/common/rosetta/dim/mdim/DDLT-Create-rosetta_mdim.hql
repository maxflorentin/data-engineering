CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.rosetta_mdim(
       segmentation_code    string,
       attribute_code       string,
       name                 string,
       second_name          string,
       hierarchical_level   string,
       end_date             string,
       unit_code            string
)
PARTITIONED BY (
    domain_code string,
    partition_date string)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
     'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
    '/santander/bi-corp/common/rosetta/dim/mdim'