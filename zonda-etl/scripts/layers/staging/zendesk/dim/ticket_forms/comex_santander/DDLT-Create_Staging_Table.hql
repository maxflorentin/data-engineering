CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.zendesk_tk_forms_comex_santander
    (
       id                      string,
       url                     string,
       name                    string,
       raw_name                string,
       display_name            string,
       raw_display_name        string,
       end_user_visible        string,
       position                string,
       ticket_field_ids        string,
       active                  string,
       default                 string,
       created_ats             string,
       updated_ats             string,
       in_all_brands           string,
       restricted_brand_ids    string,
       end_user_conditions     string,
       agent_conditions		string

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
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/zendesk/dim/tk_forms/comex-santander'