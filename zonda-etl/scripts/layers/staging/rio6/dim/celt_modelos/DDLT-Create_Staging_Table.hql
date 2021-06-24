CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_celt_modelos(
    cemo_cema_cd_marca         string,
    cemo_cd_modelo             string,
    cemo_de_modelo             string,
    cemo_pr_mercado            string,
    cemo_fe_vigencia           string,
    cemo_fe_alta               string,
    cemo_st_modelo             string,
    cemo_cd_modelo_comercial   string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/dim/celt_modelos';