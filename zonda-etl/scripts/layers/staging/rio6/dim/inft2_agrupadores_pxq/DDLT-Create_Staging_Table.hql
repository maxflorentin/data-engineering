CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_inft2_agrupadores_pxq(
    inpq_fe_informacion        string,
    inpq_inag_cd_agrupador     string,
    inpq_mt_pea                string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/dim/inft2_agrupadores_pxq';