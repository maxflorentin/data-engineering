CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.sge_grupos_economicos(
         nro_grupo string,
         nup string,
         facturacion string,
         total_activo string,
         fecha_ejercicio string,
         actividad_bcra string
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/manual/sge/grupos_economicos';