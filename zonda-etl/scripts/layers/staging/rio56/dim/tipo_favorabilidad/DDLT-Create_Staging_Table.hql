CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio56_tipo_favorabilidad (
    cod_entidad  string,
    cod_tipo_favorabilidad  string,
    desc_tipo_favorabilidad  string,
    est_tipo_favorabilidad  string,
    user_alt_tpo_favorabilidad  string,
    fec_alt_tpo_favorabilidad  string,
    user_modf_tpo_favorabilidad  string,
    fec_modf_tpo_favorabilidad  string,
    sector_owner  string
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio56/dim/tipo_favorabilidad'