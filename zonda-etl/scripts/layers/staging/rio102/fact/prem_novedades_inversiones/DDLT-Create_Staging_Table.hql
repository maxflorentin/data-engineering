CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio102_prem_novedades_inversiones(
        id_premiacion string,
        nup string,
        tipo_dni string,
        dni string,
        apellido string,
        nombre string,
        mail string,
        importe string,
        fecha_inicial string,
        fecha_final string,
        id_solicitud string,
       --  fecha_proc string,
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
    '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio102/fact/rio102_prem_novedades_inversiones';