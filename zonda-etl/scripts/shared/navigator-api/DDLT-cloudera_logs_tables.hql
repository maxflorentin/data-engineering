DROP TABLE IF EXISTS bi_corp_staging.cloudera_logs_tables;
CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.cloudera_logs_tables
(
    database_name string,
    table_name   string,
    created_at string
)
PARTITIONED BY (partition_date string)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\n'
    STORED AS TEXTFILE
    LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/cloudera/cloudera_logs_tables'
;