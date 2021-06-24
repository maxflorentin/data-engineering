CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.crm_carte_ejec_remoto
(
USER_ID         STRING,
NUP             STRING,
GRUPO           STRING,
FEC_DESDE       STRING,
FEC_HASTA       STRING,
PRESTADO        STRING,

)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/crm/fact/carte_ejec_remoto'