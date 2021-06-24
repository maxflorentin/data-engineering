CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.academia_view_courses_data (
     user_id string,
        nombre_curso string,
        categoria string,
        duracion string,
        completado_en string,
        legajo string,
        curso_id string,
        completado string
)
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/academia/academia_view_courses_data';