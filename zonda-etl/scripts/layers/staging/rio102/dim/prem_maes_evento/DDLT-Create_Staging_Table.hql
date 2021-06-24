CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio102_prem_maes_evento(
        id_evento string,
        descripcion string,
        fecha_desde string,
        fecha_hasta string,
        descripcion_corta string
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
 STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
 OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
 LOCATION
    '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio102/dim/rio102_prem_maes_evento';