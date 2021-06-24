CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio56_subconcepto (
    cod_entidad  string,
    cod_subcpto  string,
    desc_subcpto  string,
    desc_detall_subcpto  string,
    est_subcpto  string,
    user_alt_subcpto  string,
    fec_alt_subcpto  string,
    user_modf_subcpto  string,
    fec_modf_subcpto  string,
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio56/dim/subconcepto'