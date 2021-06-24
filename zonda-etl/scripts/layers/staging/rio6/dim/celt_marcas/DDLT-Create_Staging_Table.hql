CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_celt_marcas(
    cema_cd_marca  string,
    cema_de_marca  string,
    cema_fe_alta   string,
    cema_st_marca  string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/dim/celt_marcas';