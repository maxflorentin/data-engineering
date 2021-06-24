CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio258_cp_clientes(
        cod_cliente string ,
        nup string ,
        nombres string ,
        apellidos string ,
        cod_tipo_doc string ,
        nro_documento string ,
        fecha_nacimiento string ,
        cod_genero string ,
        -- ultima_modif string ,
        creado_por string ,
        ultima_modif_por string)
    PARTITIONED BY (
      ultima_modif string)
    ROW FORMAT SERDE
      'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio258/fact/rio258_cp_clientes'