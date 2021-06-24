CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.zendesk_brands_puc_santander
    (
             id					string,
             url				    string,
             name				    string,
      	     brand_url		    string,
             subdomain		    string,
             has_help_center	    string,
      	     help_center_state	string,
             active				string,
             default              string,
             is_deleted           string,
             logo                 string,
             ticket_form_ids      string,
             signature_template   string,
             created_ats          string,
             updated_ats          string,
             host_mappings        string
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
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/zendesk/dim/brands/puc-santander'