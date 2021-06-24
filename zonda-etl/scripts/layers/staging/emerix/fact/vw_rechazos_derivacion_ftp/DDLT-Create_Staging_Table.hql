CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.emerix_rechazos_derivacion
    (
        zona_suc_adm string,
        suc_adm string,
        sucursal_nombre string,
        posicion string,
        tipo_doc string,
        num_doc string,
        gestor string,
        nombre_gestor string,
        num_cliente string,
        apellido_cliente string,
        nombre_cliente string,
        banca string,
        califacion string,
        fecha_ingreso_emerix string,
        riesgo_total string,
        contrato string,
        estado_contable string,
        alta_cuenta string,
        marca string,
        sub_marca string,
        garantia string,
        fecha_vencimiento_cuenta string,
        deuda_venc string,
        codigo_estado string,
        nombre_estado string,
        codigo_escenario string,
        nombre_escenario string,
        dias_mora_contrato string,
        cantidad_dias_en_estado string,
        cantidad_dias_en_escenario string,
        prod string,
        segmento string,
        paquete string
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
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/emerix/fact/emerix_rechazos_derivacion'