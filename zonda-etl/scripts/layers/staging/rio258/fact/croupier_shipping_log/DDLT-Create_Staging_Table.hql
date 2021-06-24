CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio258_croupier_shipping_log
    (
        id string ,
        shipping_id string ,
        new_shipping_status string ,
        old_shipping_status string ,
  --       created_date string ,
        motive string ,
        branch_code string ,
        office_user string ,
        derived_branch_code string
        )
    PARTITIONED BY (
      created_date string)
    ROW FORMAT SERDE
      'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio258/fact/rio258_croupier_shipping_log'