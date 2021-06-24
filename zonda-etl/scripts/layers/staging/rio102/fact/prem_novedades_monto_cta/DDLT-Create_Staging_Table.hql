CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio102_prem_novedades_monto_cta(
        id_premiacion string,
        nro_cuenta string,
        sucursal string,
        importe string,
      --  fecha_proc string,
        id_solicitud string,
        nup string,
        estado string,
        observaciones string,
        fecha_impacto string,
        estado_impacto string,
        nro_cuota_da string,
        fecha_vencimiento_da string,
        fecha_actualizacion string
)
PARTITIONED BY (fecha_proc string)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
 STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
 OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
 LOCATION
    '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio102/fact/rio102_prem_novedades_monto_cta';