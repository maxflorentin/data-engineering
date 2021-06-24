CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio56_info_requerida_valores_hijos (
    cod_entidad  string,
    cod_info_reque_p  string,
    cod_valor_p  string,
    nro_valor_p  string,
    cod_info_reque_h  string,
    cod_valor_h  string,
    nro_valor_h  string,
    desc_valor  string,
    est_valor  string,
    user_alt_valor  string,
    fec_alt_valor  string,
    user_modf_valor  string,
    fec_modf_valor  string,
    cod_valor_info_h  string,
    cod_valor_info_p  string
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio56/dim/info_requerida_valores_hijos'