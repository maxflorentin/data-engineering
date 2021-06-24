CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.normalizacion(
  num_sec string,
  entidad string,
  centro string,
  contrato string,
  producto string,
  subproducto string,
  divisa string,
  fec_apertura string,
  marca_seg_act string,
  fec_cambio_seg string,
  fec_cura string,
  fec_normalizacion string,
  num_ree string
)
PARTITIONED BY (
  partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/garra/fact/normalizacion'
