CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_risk.raw_entradas(
            periodo double ,
            nup double ,
            sucursal double ,
            nro_cuenta double ,
            codigo_producto string ,
            codigo_subproducto string ,
            divisa string ,
            fecha_situacion_irregular string ,
            dias_atraso string ,
            marca string ,
            submarca string ,
            importe_entrada double ,
            tipo_movimiento string ,
            codigo_segmento string ,
            descripcion_segmento string ,
            detalle_renta string ,
            tipo_producto string ,
            descripcion_producto string ,
            tipo_reestructuracion string ,
            contrato_citi boolean )
        PARTITIONED BY (
          fecha_ejecucion_garra string)
        ROW FORMAT SERDE
          'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
        STORED AS INPUTFORMAT
          'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
        OUTPUTFORMAT
          'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
        LOCATION
          '${DATA_LAKE_SERVER}/santander/bi-corp/sandbox/rda/staging/entradas'