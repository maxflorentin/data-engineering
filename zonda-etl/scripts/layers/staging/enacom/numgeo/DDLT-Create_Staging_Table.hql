CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.enacom_num_geo(
       OPERADOR string,
       SERVICIO string,
       MODALIDAD string,
       LOCALIDAD string,
       INDICATIVO string,
       BLOQUE string,
       RESOLUCION string,
       FECHA string
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/enacom/fact/numgeo/';