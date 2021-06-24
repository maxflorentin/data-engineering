CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.emerix_actuaciones
    (
        numero_proceso string,
        contrato string,
        fecha_actuacion string,
        tipo_actuacion string,
        codigo_estado string,
        nombre_estado string,
        codigo_escenario string,
        nombre_escenario string,
        usuario string,
        num_cliente string,
        bufette string
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
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/emerix/fact/emerix_actuaciones'