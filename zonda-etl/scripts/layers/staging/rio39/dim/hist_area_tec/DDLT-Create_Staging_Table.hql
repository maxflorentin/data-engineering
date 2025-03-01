CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.comex_rio39_hist_area_tec (
NRO_FORM            STRING,
COM_ENVIO           STRING,
COM_RESPUESTA       STRING,
USU_ENVIO           STRING,
FECHA_ENVIO         STRING,
USU_RESPUESTA       STRING,
FECHA_RESPUESTA     STRING
)
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio39/dim/hist_area_tec';