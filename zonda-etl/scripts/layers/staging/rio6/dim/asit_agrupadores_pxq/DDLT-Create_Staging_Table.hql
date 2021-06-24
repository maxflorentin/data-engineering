CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_asit_agrupadores_pxq(
    aspq_fe_informacion   string,
    aspq_cd_agrupador     string,
    aspq_mt_pea           string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/dim/asit_agrupadores_pxq';