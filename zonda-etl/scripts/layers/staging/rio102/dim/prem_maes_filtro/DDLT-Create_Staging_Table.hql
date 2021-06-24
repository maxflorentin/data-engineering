CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio102_prem_maes_filtro(
        id_filtro string,
        descripcion string,
        fecha_desde string,
        fecha_hasta string,
        usuario_creacion string,
        id_filtro_pres string,
        grupo string
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
 STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
 OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
 LOCATION
    '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio102/dim/rio102_prem_maes_filtro';