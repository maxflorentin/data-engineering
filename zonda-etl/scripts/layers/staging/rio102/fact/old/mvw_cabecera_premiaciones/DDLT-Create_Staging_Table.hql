CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio102_mvw_cabecera_premiaciones(
        id_evento string ,
        interfaces string ,
        descripcion string ,
        descripcion_corta string ,
      --   fecha_desde string ,
        fecha_hasta string ,
        id_premiacion string ,
        id_interfaz string ,
        maestro_descripcion string ,
        maestro_fecha_desde string ,
        maestro_fecha_hasta string ,
        maestro_tabla_novedad string )
    PARTITIONED BY (
      fecha_desde string)
    ROW FORMAT SERDE
      'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio102/fact/rio102_mvw_cabecera_premiaciones'