CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio3_circuito(
    codigo string,
    descri string,
    dias_backoffice string,
    activo string,
    tipo_informe_entrevista string,
    codificacion_oca string,
    pais string,
    tiposolicitud string,
    procesotibco string,
    tipocircuito string,
    reversa string
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio3/dim/circuito';