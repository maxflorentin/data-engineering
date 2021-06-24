CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_risk.raw_inventario(
        periodo string,
        nup string,
        sucursal string,
        nro_cuenta string,
        codigo_producto string,
        codigo_subproducto string,
        divisa string,
        fecha_situacion_irregular string ,
        fecha_alta_producto string ,
        fecha_vencimiento_producto string ,
        dias_atraso string ,
        dias_atraso_calculado string ,
        marca string ,
        submarca string ,
        codigo_segmento string ,
        descripcion_segmento string ,
        detalle_renta string ,
        tipo_producto string ,
        descripcion_producto string ,
        tipo_reestructuracion string ,
        motivo_riesgo_sub_est string ,
        clasificacion_reestructuracion string ,
        fecha_clasificacion_reestructuracion string ,
        via_ingreso string ,
        importe_riesgo string ,
        importe_irregular string ,
        contrato_citi boolean)
      PARTITIONED BY (
        fecha_ejecucion_garra string)
      ROW FORMAT SERDE
        'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
      STORED AS INPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
      OUTPUTFORMAT
        'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
      LOCATION
        '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/inventario'