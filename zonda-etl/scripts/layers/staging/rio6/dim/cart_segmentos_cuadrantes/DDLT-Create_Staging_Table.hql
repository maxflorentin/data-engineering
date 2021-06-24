CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_cart_segmentos_cuadrantes(
    cagc_casg_segmento       string,
    cagc_casg_seg_calculado  string,
    cagc_cd_cuadrante        string,
    cagc_de_cuadrante        string,
    cagc_de_segmento         string,
    cagc_st_segmento         string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/dim/cart_segmentos_cuadrantes';