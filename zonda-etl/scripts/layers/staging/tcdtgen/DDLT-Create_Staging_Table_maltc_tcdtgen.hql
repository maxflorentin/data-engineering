CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.tcdtgen
(
gentabla string
,gen_entidad string
,gen_idioma string
,gen_clave string
,gen_datos string
,gen_timest_alt string
,gen_usuario_alt string
,gen_timest_mod string
,gen_usuario_mod string
)
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/maltc/dim/tcdtgen';
