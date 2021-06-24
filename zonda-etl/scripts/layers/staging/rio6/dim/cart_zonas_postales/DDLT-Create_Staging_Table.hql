CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_cart_zonas_postales(
    cazp_caes_cd_estado            string,
    cazp_cd_poblacion              string,
    cazp_cd_colonia                string,
    cazp_cd_postal                 string,
    cazp_cd_zona_sismica           string,
    cazp_cd_zona_huracan           string,
    cazp_caci_cd_ciudad            string,
    cazp_cazz_cd_cumulo            string,
    cazp_cnir_cd_nivel_riesgo      string,
    cazp_cnir_cd_nivel_riesgo_22   string,
    cazp_cnir_cd_nivel_riesgo_23   string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/dim/cart_zonas_postales';