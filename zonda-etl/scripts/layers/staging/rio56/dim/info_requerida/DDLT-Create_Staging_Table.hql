CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio56_info_requerida (
    cod_entidad  string,
    cod_info_reque  string,
    desc_info_reque  string,
    cod_tpo_dat  string,
    long_info_reque  string,
    cant_dec_info_reque  string,
    rang_dde_info_reque  string,
    rang_hta_info_reque  string,
    fec_dde_info_reque  string,
    fec_hta_info_reque  string,
    est_info_reque  string,
    user_alt_info_reque  string,
    user_modf_info_reque  string,
    fec_alt_info_reque  string,
    fec_modf_info_reque  string,
    cod_tpo_campo  string,
    cod_funcion_asoc  string,
    cod_info_relac  string,
    tpo_info_especial  string,
    sector_owner  string,
    texto_ayuda  string,
    cod_info_condic  string,
    cod_valor_condic  string
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio56/dim/info_requerida'