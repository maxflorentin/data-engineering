CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio258_shipping_event
    (
        id string,
        internal_shipping_id string,
        status_id string,
        event_date string,
        branch_code string,
        courier_branch_code string,
        uuid string,
        processed_date string
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
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio258/fact/rio258_shipping_event'