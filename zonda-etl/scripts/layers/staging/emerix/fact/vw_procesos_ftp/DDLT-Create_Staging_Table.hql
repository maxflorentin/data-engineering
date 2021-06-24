CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.emerix_procesos
    (
        num_cliente string,
        contrato string,
        codigo_estado string,
        nombre_estado string,
        codigo_escenario string,
        nombre_escenario string,
        codigo_agencia string,
        nombre_agecia string,
        numero_proceso string,
        estado_proceso string,
        fecha_alta_proceso string,
        fecha_fin_proceso string,
        tipo_proceso string,
        juzgado string,
        secretaria string,
        expediente string,
        riesgo_contrato string,
        num_reg string
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
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/emerix/fact/emerix_procesos'