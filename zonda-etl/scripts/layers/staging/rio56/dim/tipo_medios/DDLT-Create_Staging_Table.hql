CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio56_tipo_medios (
    cod_entidad  string,
    tpo_medio  string,
    desc_medio  string,
    desc_detall_medio  string,
    indi_tpo_medio  string,
    est_medio  string,
    user_alt_med  string,
    fec_alt_med  string,
    user_modf_med  string,
    fec_modf_med  string,
    sector_owner  string,
    indi_visible  string,
    indi_msj_asoc  string
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
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio56/dim/tipo_medios'