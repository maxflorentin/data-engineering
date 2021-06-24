CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.zendesk_gr_comex_santander
    (
id              string,
url             string,
name            string,
description     string,
default         string,
deleted         string,
created_at      string,
updated_at		string
        )
    PARTITIONED BY (
      partition_date string)
    ROW FORMAT SERDE
      'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/zendesk/dim/gr/comex-santander'