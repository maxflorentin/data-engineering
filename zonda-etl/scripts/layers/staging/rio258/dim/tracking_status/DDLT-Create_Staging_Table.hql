CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio258_tracking_status
    (
        id string ,
        code string ,
        reason string ,
        secondary_reason string ,
        courier_status_code string ,
        courier string
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
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio258/dim/rio258_tracking_status'