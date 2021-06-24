CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio258_shipping
    (
        id string,
        shipping_id string,
        tracking_id string,
        courier string,
        contract string,
        customer_full_name string,
        customer_document_number string,
        customer_address_street string,
        customer_address_number string,
        customer_address_floor string,
        customer_address_department string,
        customer_address_city string,
        customer_address_postal_code string,
        customer_address_province string,
        branch_code string,
        creation_date string,
     --   last_update
        canceled string
        )
    PARTITIONED BY (
      last_update string)
    ROW FORMAT SERDE
      'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio258/fact/rio258_shipping'