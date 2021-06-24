CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio347_balance(
    securities_contract_code   string,
    office_code     string,
    assetid         string,
    nominals        string,
    holding_status  string,
    place_holding   string,
    value_date      string,
    asset_price     string,
    creation_date   string,
    update_date     string
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio347/fact/balance';