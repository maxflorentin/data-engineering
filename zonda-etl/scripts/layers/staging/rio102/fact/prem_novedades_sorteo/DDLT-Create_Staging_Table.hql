CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio102_prem_novedades_sorteo(
        nup string,
        id_premiacion string,
        chances_acum string,
        periodo string,
        id_solicitud string,
       -- fecha_proc string,
        acreditacion_pas string,
        consumo_td string,
        consumo_tc_visa string,
        consumo_tc_amex string,
        tipo_acumulacion string,
        id_solicitud_ctx string,
        nro_cuenta string,
        sucursal string,
        tipo_registro string,
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
    '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio102/fact/rio102_prem_novedades_sorteo';