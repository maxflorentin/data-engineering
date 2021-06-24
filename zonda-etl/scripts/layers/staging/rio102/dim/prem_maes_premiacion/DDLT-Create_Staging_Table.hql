CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio102_prem_maes_premiacion(
        id_premiacion string,
        fecha_desde string,
        fecha_hasta string,
        fecha_asignacion string,
        usuario string,
        id_objeto_log string,
        descripcion string,
        tabla_novedades string,
        id_ejecucion string,
        descrip_prem_canal string,
        leyenda_prem_canal string
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
 STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
 OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
 LOCATION
    '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio102/dim/rio102_prem_maes_premiacion';