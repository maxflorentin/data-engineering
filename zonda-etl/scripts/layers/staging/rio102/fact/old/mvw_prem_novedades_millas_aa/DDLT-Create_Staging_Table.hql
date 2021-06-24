CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio102_mvw_prem_novedades_millas_aa(
     id_premiacion string ,
    nup string ,
    tipo_dni string ,
    dni string ,
    signo string ,
    millas string ,
    --fecha_proc string ,
    id_solicitud string ,
    estado string ,
    observaciones string ,
    nup_referido string ,
    comercio string ,
    fecha_de_consumo string ,
    fecha_de_cierre string ,
    millas_acumuladas string ,
    cant_consumo_tc string ,
    numero_loyal string ,
    fecha_impacto string ,
    estado_impacto string ,
    fecha_actualizacion string,
    id_evento string
    )
    PARTITIONED BY (
      fecha_proc string)
    ROW FORMAT SERDE
      'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio102/fact/rio102_mvw_prem_novedades_millas_aa'