CREATE EXTERNAL TABLE IF NOT EXISTS BI_CORP_COMMON.BT_MYA_NOVEDADESENVIADAS (
COD_MYA_MENSAJE                         STRING,
COD_MYA_MENSAJE_REQ                     STRING,
COD_MYA_NOVEDAD                         STRING,
DS_MYA_NOVEDAD                          STRING,
TS_MYA_FECHA_CREACION                   STRING,
FLAG_MYA_MANUAL                         INT,
FLAG_MYA_PROCESADO                      INT,
DS_MYA_MENSAJE                          STRING,
TS_MYA_INICIO_PROCESAMIENTO             STRING,
COD_PER_NUP                             BIGINT,
DS_MYA_DOCUMENTO                        BIGINT,
TS_MYA_FECHA_LOG                        STRING,
COD_MYA_DESTINO                         STRING,
COD_MYA_ESTADO                          STRING,
DS_MYA_ESTADO                           STRING,
COD_MYA_DISPOSITIVO                     STRING,
COD_MYA_NOTIFICACION                    STRING
)
PARTITIONED BY (PARTITION_DATE STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/mya/fact/bt_mya_novedades_enviadas'