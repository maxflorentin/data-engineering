CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio56_tipo_gestion (
    cod_entidad  string,
    tpo_gestion  string,
    desc_tpo_gest  string,
    desc_detall_tpo_gest  string,
    est_tpo_gest  string,
    user_alt_tpo_gest  string,
    fec_alt_tpo_gest  string,
    user_modf_tpo_gest  string,
    fec_modf_tpo_gest  string
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio56/dim/tipo_gestion'