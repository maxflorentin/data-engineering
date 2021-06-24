CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio157_ms0_dt_re_cta_agrp_func (
  	cod_pais string, 
    cod_entidad_espana string, 
    cod_cta_cont string, 
    cod_agrp_func string, 
    fec_alta string, 
    fec_baja string, 
    ind_generado string, 
    userid_umo string, 
    timest_umo string
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
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio157/dim/ms0_dt_re_cta_agrp_func'