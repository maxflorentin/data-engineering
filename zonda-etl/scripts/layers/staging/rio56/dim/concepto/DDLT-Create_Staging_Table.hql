CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio56_concepto (
    cod_entidad  string,
    cod_cpto  string,
    desc_cpto  string,
    desc_detall_cpto  string,
    est_cpto  string,
    user_alt_cpto  string,
    fec_alt_cpto  string,
    user_modf_cpto  string,
    fec_modf_cpto  string,
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio56/dim/concepto'