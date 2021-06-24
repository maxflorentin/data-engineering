CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_cart_actividades_economicas(
    caae_cd_actividad         string,
    caae_de_actividad         string,
    caae_cd_indole_categoria  string,
    caae_st_actividad         string,
    caae_mca_sujeto_oblig     string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}//santander/bi-corp/staging/rio6/dim/cart_actividades_economicas';