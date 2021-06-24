CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_inft2_agrupadores(
    inag_cd_agrupador  string,
    inag_de_agrupador  string,
    inag_st_agrupador  string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/dim/inft2_agrupadores';