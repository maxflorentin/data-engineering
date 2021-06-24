CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio56_producto (
    cod_entidad  string,
    cod_prod  string,
    desc_prod  string,
    est_prod  string,
    user_alt_prod  string,
    fec_alt_prod  string,
    user_modf_prod  string,
    fec_modf_prod  string
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio56/dim/producto'