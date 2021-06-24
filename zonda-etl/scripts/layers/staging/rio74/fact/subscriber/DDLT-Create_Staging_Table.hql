CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio74_subscriber(
ID_SUBSCRIBER         STRING,
ID_STATUS             STRING,
IDTYPE                STRING,
IDNUMBER              STRING,
GIVENNAME             STRING,
SURNAME               STRING,
TELNUMBER             STRING,
LEGALTYPE             STRING,
BIRTHDATE             STRING,
ID_CHANNELBANK        STRING,
ID_SUBCHANNELBANK     STRING,
PRIVATEBANK           STRING,
PERSONALBANK          STRING,
CREATE_DATE           STRING,
MODIFY_DATE           STRING,
ID_CHANNELBANK_INI    STRING,
ID_SUBCHANNELBANK_INI STRING,
ID_OPERADOR           STRING,
TYC_WHATSAPP          STRING
)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio74/fact/subscriber';