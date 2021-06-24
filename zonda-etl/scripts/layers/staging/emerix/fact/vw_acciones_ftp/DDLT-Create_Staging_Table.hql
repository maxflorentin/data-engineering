CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.emerix_acciones
    (
       nombre_accion string,
        nombre_respuesta string,
        fecha_accion string,
        fecha_respuesta string,
        num_cliente string,
        contrato string,
        codigo_estado string,
        nombre_estado string,
        codigo_escenario string,
        nombre_escenario string,
        usuario string,
        buffete string,
        tipo_reg string
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
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/emerix/fact/emerix_acciones'