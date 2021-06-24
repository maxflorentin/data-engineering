CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio102_prem_novedades_arch_delivery(
        id_premiacion string,
        dni string,
        sexo string,
        nro_cuenta string,
        sucursal string,
        nup string,
      --  fecha_proc string,
        nombre string,
        apellido string,
        suscriptor string,
        producto string,
        marca_upgr string,
        id_solicitud string,
        fecha_impacto string,
        estado_impacto string
)
PARTITIONED BY (fecha_proc string)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
 STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
 OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
 LOCATION
    '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio102/fact/rio102_prem_novedades_arch_delivery';