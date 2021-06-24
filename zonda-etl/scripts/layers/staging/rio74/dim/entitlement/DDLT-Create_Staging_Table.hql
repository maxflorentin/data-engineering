CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio74_entitlement(
ID_ENTITLEMENT                     STRING,
ENTITLEMENT                        STRING,
DESCRIPTION                        STRING,
LONG_DESCRIPTION                   STRING,
LABEL                              STRING,
DEFAULT_FREQUENCY                  STRING,
DEFAULT_TIMEFRAME                  STRING,
DEFAULT_DAYS_TO_MATURITY           STRING,
ID_STATUS                          STRING,
VISIBLE                            STRING,
VISIBLE_SEARCH                     STRING,
DESCRIPTION_SEARCH                 STRING,
FORMAT_INPUT                       STRING,
PRIORITY                           STRING,
ID_SUBCHANNELBANK_DEFAULT          STRING,
ID_CHANNELBANK_DEFAULT             STRING,
SAMPLE_INPUT                       STRING,
ID_RULE                            STRING,
ID_PROVIDER                        STRING,
TTL                                STRING,
PAGE_SHOW                          STRING,
NEWSLETTER                         STRING,
ID_FILTER                          STRING,
TIPO_ESPECIAL                      STRING,
ACNL                               STRING,
URL                                STRING
)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio74/dim/entitlement';