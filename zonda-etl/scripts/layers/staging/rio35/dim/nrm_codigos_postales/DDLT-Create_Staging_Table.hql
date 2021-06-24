CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio35_nrm_codigos_postales(
LOCALIDAD             STRING,
PREFIJO_INTERURBANO       STRING,
CODIGO_POSTAL       STRING

)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio35/dim/nrm_codigos_postales';