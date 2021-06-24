CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio258_cp_envios_componentes_estados
        (
        cod_envio_comp_estado string ,
        cod_componente string ,
        cod_estado string ,
        fecha_novedad string ,
        -- ultima_modif string ,
        creado_por string ,
        ultima_modif_por string ,
        cod_error string ,
        desc_error string
        )
    PARTITIONED BY (
      ultima_modif string)
    ROW FORMAT SERDE
      'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio258/fact/rio258_cp_envios_componentes_estados'