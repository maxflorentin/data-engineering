CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio102_mvw_prem_nov_monto_cta_tc(
    id_premiacion string ,
    tipo_tarjeta string ,
    cuenta_tc string ,
    cartera string ,
    nup string ,
    nro_tarjeta string ,
    importe string ,
    id_solicitud string ,
   -- fecha_proc string ,
    estado string ,
    observaciones string ,
    cant_cierre string ,
    fecha_impacto string ,
    estado_impacto string ,
    nro_cuota string ,
    nro_cuota_da string ,
    fecha_vencimiento_da string ,
    fecha_actualizacion string ,
    id_evento string
)
    PARTITIONED BY (
      fecha_proc string)
    ROW FORMAT SERDE
      'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
    STORED AS INPUTFORMAT
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
    OUTPUTFORMAT
      'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
    LOCATION
      '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio102/fact/rio102_mvw_prem_nov_monto_cta_tc'