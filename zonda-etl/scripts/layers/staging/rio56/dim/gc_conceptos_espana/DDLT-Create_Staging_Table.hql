CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio56_gc_conceptos_espana (
    cod_entidad  string,
    cod_cpto  string,
    desc_cpto  string,
    cod_tipo_reclamo  string,
    user_alt  string,
    fec_alt  string,
    user_modf  string,
    fec_modf  string,
    estado  string
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
  '/santander/bi-corp/staging/rio56/dim/gc_conceptos_espana'