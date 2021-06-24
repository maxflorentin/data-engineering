CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio102_prem_clientes_campania(
        id_solicitud string ,
        fecha_desde string ,
        fecha_hasta string ,
        descripcion_campania string ,
        id_premiacion string ,
        descripcion_premiacion string ,
        id_evento string ,
        descripcion_evento string ,
        fecha_proceso string ,
        nup string ,
        sucursal string ,
        nro_cuenta string ,
        cuenta_tc string ,
        tipo_tc string ,
        tipo_premiacion string ,
        premio string ,
        fecha_desde_camp string ,
        fecha_hasta_camp string ,
        observaciones string ,
        fecha_carga string ,
        codigo_ajuste string
)
PARTITIONED BY (periodo string)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
 STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
 OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
 LOCATION
    '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio102/fact/rio102_prem_clientes_campania';