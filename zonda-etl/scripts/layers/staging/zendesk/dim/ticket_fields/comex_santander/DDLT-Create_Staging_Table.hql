CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.zendesk_tk_fields_comex_santander
    (
       id                          string,
       url                         string,
       title                       string,
       raw_title                   string,
       description                 string,
       raw_description             string,
       position                    string,
       active                      string,
       required                    string,
       collapsed_for_agents        string,
       regexp_for_validation       string,
       title_in_portal             string,
       raw_title_in_portal         string,
       visible_in_portal           string,
       editable_in_portal          string,
       required_in_portal          string,
       tag                         string,
       created_at                  string,
       updated_at                  string,
       removable                   string,
       agent_description			string

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
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/zendesk/dim/tk_fields/comex-santander'