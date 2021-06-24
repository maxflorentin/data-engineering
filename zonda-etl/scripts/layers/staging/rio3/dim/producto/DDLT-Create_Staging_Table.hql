CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio3_producto(
    CODIGO                     STRING,
    DESCRI                     STRING,
    PRODUCTO_ASOL              STRING,
    ICONO                      STRING,
    VERENARBOL                 STRING,
    VISIBLE_GRILLA             STRING,
    ACTIVO                     STRING,
    DESCRI_CRM                 STRING,
    OFRECER                    STRING,
    FAMILIA                    STRING,
    AUTOMATICO                 STRING,
    ORDEN                      STRING,
    CELLSTATION                STRING,
    PRIORIDAD                  STRING,
    INTERNET                   STRING,
    PATHICONO                  STRING,
    PROVEEDORID                STRING,
    PAIS                       STRING,
    VERSION_ESQUEMA            STRING,
    TIPOINFINITY               STRING,
    DIAS_RECORDATORIO_INTERNET STRING,
    IDENTIFICARIVR             STRING,
    VALIDA_SCORING             STRING,
    CANT_MAX_DIAS_DESBLOQUEO   STRING
)

PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio3/dim/producto';

