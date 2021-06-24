CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_cart_ciudades(
    caci_caes_cd_estado   string,
    caci_cd_ciudad        string,
    caci_de_ciudad        string,
    caci_cd_zona_sismica  string,
    caci_st_ciudad        string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/dim/cart_ciudades';