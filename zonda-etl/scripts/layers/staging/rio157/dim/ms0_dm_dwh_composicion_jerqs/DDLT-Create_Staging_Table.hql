CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio157_ms0_dm_dwh_composicion_jerqs (
    cod_dimension  string,
    cod_jerarquia  string,
    cod_valor  string,
    cod_valor_padre  string,
    num_orden_despliegue  string,
    num_nivel  string,
    fec_baja  string,
    fec_alta  string,
    userid_umo  string,
    timest_umo  string,
    tip_elemento  string,
    val_signo_agreg  string,
    cod_correlativo  string,
    cod_pais  string,
    primer_padre_corp  string
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
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio157/dim/ms0_dm_dwh_composicion_jerqs'