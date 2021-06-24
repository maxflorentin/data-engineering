CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio102_prem_eventos_sql(
        id_evento string,
        orden_id string,
        condicion string,
        sql string,
        descripcion string,
        interfaces string,
        origen_datos string,
        fecha_ult_modificacion string,
        origen_info string,
        tipo_interfaz string
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
 STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
 OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
 LOCATION
    '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio102/dim/rio102_prem_eventos_sql';