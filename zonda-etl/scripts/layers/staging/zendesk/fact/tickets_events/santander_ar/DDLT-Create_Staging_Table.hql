CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.zendesk_tk_events_santander_ar
        (
       id	                        string,
       child_events					        string,
       ticket_id				string,
       timestamp					        string,
       created_at					string,
       updater_id						    string,
       via				    string,
       system					    string,
       event_type  			    string
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
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/zendesk/fact/tk_events/santander-ar'