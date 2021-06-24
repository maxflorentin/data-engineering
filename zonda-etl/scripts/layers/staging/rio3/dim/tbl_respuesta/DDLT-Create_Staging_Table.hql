CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio3_tbl_respuesta(
encuesta        string,
pregunta        string,
respuesta       string,
proximapregunta string,
descri          string,
activo          string,
comentario      string,
valor           string,
puntaje         string,
respuestalibre  string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio3/dim/tbl_respuesta';