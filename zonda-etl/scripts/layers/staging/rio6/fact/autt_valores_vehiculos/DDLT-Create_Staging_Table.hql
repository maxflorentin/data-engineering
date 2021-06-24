CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_autt_valores_vehiculos(
    auvv_auma_cd_marca   string,
    auvv_aumo_cd_modelo  string,
    auvv_version         string,
    auvv_ano             string,
    auvv_valor           string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/fact/autt_valores_vehiculos';