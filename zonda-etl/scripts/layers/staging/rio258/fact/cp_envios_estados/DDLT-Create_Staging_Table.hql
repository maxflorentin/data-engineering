CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio258_cp_envios_estados(

        cod_envio_estado string ,
        crupier_id string ,
        cod_estado string ,
        fecha_estado string ,
        -- ultima_modif string ,
        fecha_estado_solr string ,
        creado_por string,
        ultima_modif_por string
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
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio258/fact/rio258_cp_envios_estados'