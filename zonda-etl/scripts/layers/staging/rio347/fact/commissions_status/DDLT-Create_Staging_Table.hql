CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio347_commissions_status(
    customerid                   string,
    securities_contract_code     string,
    office_code                  string,
    origin                       string,
    actionid                     string,
    period                       string,
    payment_date                 string,
    creation_date                string,
    update_date                  string
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio347/fact/commissions_status';