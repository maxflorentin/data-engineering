CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.academia_view_plan_course_all (
     user_id  string,
        career_plan string,
        course string,
        career_plan_id string,
        course_id string,
        plan_duration_mandatory string,
        plan_duration_optional string,
        plan_duration  string,
        progress_hours_mandatory string,
        progress_hours_optional string,
        progress_hours string,
        progress_mandatory string,
        progress string,
        legajo string

)
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/academia/academia_view_plan_course_all';