CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio3_detalleresultados(
    operacion string,
    encuesta string,
    pregunta string,
    respuesta string,
    comentario string,
    fecha string
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio3/fact/detalleresultados';