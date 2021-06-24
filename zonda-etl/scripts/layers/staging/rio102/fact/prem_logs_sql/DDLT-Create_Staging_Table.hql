CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio102_prem_logs_sql(
        id_solicitud string,
        id_ejecucion string,
        id_premiacion string,
        id_evento string,
        orden_id string,
        sql string
)
PARTITIONED BY (fecha_ejecucion string)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
 STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
 OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
 LOCATION
    '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio102/fact/rio102_prem_logs_sql';