CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio3_preguntas(
    encuesta string,
    pregunta string,
    descri string,
    respuesta_multiple string,
    activo string,
    orden string,
    contabilizar string
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio3/dim/preguntas';