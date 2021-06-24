CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio3_tbl_pregunta(
encuesta           string,
pregunta           string,
descri             string,
primerapregunta    string,
activo             string,
comentario         string,
tabla_nombre       string,
tabla_codigo       string,
tabla_descri       string,
tabla_condicion    string,
respuesta_multiple string

)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio3/dim/tbl_pregunta';