CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio102_prem_novedades_cuota_seguro(
        id_premiacion string,
        nup string,
        cod_ramo string,
        nu_poliza string,
        nu_certificado string,
        tipo_cuenta string,
        nro_cuenta string,
        tipo_acred string,
        monto_cuota_seguro string,
        monto_bonificacion string,
        id_solicitud string,
        -- fecha_proc string,
        estado string,
        observaciones string,
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
    '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio102/fact/rio102_prem_novedades_cuota_seguro';