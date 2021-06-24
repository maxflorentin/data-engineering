CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.zendesk_tk_santander_ar
    (
       url	                        string,
       id	                        string,
       external_id					string,
       via							string,
       created_at					string,
       updated_at					string,
       type						    string,
       subject						string,
       raw_subject					string,
       description					string,
       priority					    string,
       status						string,
       recipient					string,
       requester_id				    string,
       submitter_id				    string,
       assignee_id					string,
       organization_id				string,
       group_id					    string,
       collaborator_ids			    string,
       follower_ids				    string,
       email_cc_ids				    string,
       forum_topic_id				string,
       problem_id					string,
       has_incidents				string,
       is_public					string,
       due_at						string,
       tags						    string,
       custom_fields				string,
       satisfaction_rating			string,
       sharing_agreement_ids		string,
       fields						string,
       followup_ids				    string,
       ticket_form_id				string,
       brand_id					    string,
       satisfaction_probability	    string,
       allow_channelback			string,
       result_type					string
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
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/zendesk/fact/tk/santander-ar'