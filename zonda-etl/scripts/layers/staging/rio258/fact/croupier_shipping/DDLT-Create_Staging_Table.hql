CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio258_croupier_shipping

(
        id string ,
        courier_id string ,
        courier_tracking_id string ,
        created_date string ,
        received_user string ,
        shipping_status string ,
        contract_code string ,
        contract_service_type string ,
        contract_description string ,
        branch_code string ,
        branch_name string ,
        branch_address string ,
        branch_city string ,
        person_document string ,
        person_name string ,
        branch_code_received string ,
        -- last_update_date string ,
        content string ,
        last_update_user string ,
        tag string ,
        person_nup string ,
        received_date string,
        tracking_shipping_id string,
        channel_code 	 string,
        channel_description  string,
        channel_operation string
       )
    PARTITIONED BY (
      last_update_date string)
    ROW FORMAT SERDE
      'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio258/fact/rio258_croupier_shipping'