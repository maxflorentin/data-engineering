CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.zendesk_usr_getnet
    (
       usr_id                              string,
       usr_url                             string,
       usr_name                            string,
       usr_email                           string,
       usr_created_at                      string,
       usr_updated_at                      string,
       usr_time_zone                       string,
       usr_iana_time_zone                  string,
       usr_phone                           string,
       usr_shared_phone_number             string,
       usr_photo                           string,
       usr_locale_id                       string,
       usr_locale                          string,
       usr_organization_id                 string,
       usr_role                            string,
       usr_verified                        string,
       usr_external_id                     string,
       usr_tags                            string,
       usr_alias                           string,
       usr_active                          string,
       usr_shared                          string,
       usr_shared_agent                    string,
       usr_last_login_at                   string,
       usr_two_factor_auth_enabled         string,
       usr_signature                       string,
       usr_details                         string,
       usr_notes                           string,
       usr_role_type                       string,
       usr_custom_role_id                  string,
       usr_moderator                       string,
       usr_ticket_restriction              string,
       usr_only_private_comments           string,
       usr_restricted_agent                string,
       usr_suspended                       string,
       usr_chat_only                       string,
       usr_default_group_id                string,
       usr_report_csv                      string,
       usr_user_fields                     string
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
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/zendesk/fact/users/globalgetnet'