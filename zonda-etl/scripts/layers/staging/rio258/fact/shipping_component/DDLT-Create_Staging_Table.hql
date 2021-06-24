CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio258_shipping_component
    (
        id string,
        internal_shipping_id string,
        code string,
        type string,
        tracking_id string,
        event_date string
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
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio258/fact/rio258_shipping_component'