CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio102_mvw_prem_novedades_camp_bonif(
        id_premiacion string ,
        nro_cuenta string ,
        sucursal string ,
        codigo_bono string ,
    --    fecha_proc string ,
        id_solicitud string ,
        estado string ,
        observaciones string ,
        fecha_act string ,
        nup string ,
        act_campania string ,
        nro_gestion string ,
        fecha_impacto string ,
        estado_impacto string ,
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
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio102/fact/rio102_mvw_prem_novedades_camp_bonif'