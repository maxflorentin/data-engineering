CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio102_prem_solicitudes_filtros(
        id_solicitud string,
        id_filtro string,
        valor_desde string,
        valor_hasta string,
--         fecha_carga string,
        id_usuario string
)
PARTITIONED BY (fecha_carga string)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
 STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
 OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
 LOCATION
    '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio102/fact/rio102_prem_solicitudes_filtros';