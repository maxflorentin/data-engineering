CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio6_asit_planes_detalle(
    aspd_cd_plan       string,
    aspd_cd_cobertura  string,
    aspd_nu_orden      string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio6/dim/asit_planes_detalle';