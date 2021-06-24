CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.emerix_clientes
    (
        apellido_cliente string,
        nombre_cliente string,
        codigo_agencia string,
        nombre_agencia string,
        cantidad_dias_en_escenario string,
        cantidad_dias_en_estado string,
        categoria_producto string,
        deuda_contrato string,
        dias_mora_contrato string,
        fecha_ini_asignacion_estudio string,
        fecha_fin_asignacion_estudio string,
        codigo_escenario string,
        nombre_escenario string,
        codigo_estado string,
        nombre_estado string,
        monto_transferido_contrato string,
        apellido_gestor string,
        nombre_gestor string,
        grupo_producto string,
        moneda string,
        motivo_salida_contrato string,
        numero_cliente string,
        numero_contrato string,
        pes_codigo string,
        banca_nombre_corto string,
        pes_obs string,
        cod_estado_contable string,
        estado_contable string,
        codigo_garantia string,
        nombre_garantia string,
        riesgo_total_persona string,
        barrio_persona string,
        cliente_citinv string,
        contrato_citi string,
        fecha_ultima_consulta_nosis string,
        ingresos_inferidos string,
        endeudamiento_1 string,
        endeudamiento_2 string,
        endeudamiento_3 string,
        cantidad_cheques_rechazados string,
        monto_cheques_rechazados string,
        tiene_juicio string,
        marca_pase_judicial string,
        marca_venta string,
        telefono_1 string,
        telefono_2 string,
        telefono_3 string,
        valor_score string,
        valor_perju string,
        valor_benef string,
        script string,
        numero_proceso string,
        numero_orden_judicial string,
        numero_posicion string
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
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/emerix/fact/emerix_clientes'