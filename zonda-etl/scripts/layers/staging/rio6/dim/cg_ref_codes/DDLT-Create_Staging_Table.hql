CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_cg_ref_codes(
    rv_low_value    string,
    rv_high_value   string,
    rv_abbreviation string,
    rv_domain       string,
    rv_meaning      string,
    rv_type         string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/dim/cg_ref_codes';