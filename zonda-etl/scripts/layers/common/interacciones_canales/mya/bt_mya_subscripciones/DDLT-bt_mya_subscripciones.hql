CREATE EXTERNAL TABLE IF NOT EXISTS BI_CORP_COMMON.BT_MYA_SUBSCRIPCIONES (
ID_MYA_SUBSCRIPCION                     STRING,
COD_MYA_NOVEDAD                         STRING,
COD_PER_NUP                             BIGINT,
COD_MYA_DISPOSITIVO                     STRING,
DS_MYA_SECUENCIA_NUM                    STRING,
COD_MYA_TIEMPO_ENVIO                    STRING,
COD_MYA_ESTADO                          STRING,
DS_MYA_CANT_DIASENVIOS                  STRING,
DS_MYA_FRECUENCIA                       STRING,
DS_MYA_OBSERVACIONES                    STRING,
FLAG_MYA_UNICA_VEZ                      INT,
TS_MYA_FECHA_CREACION                   STRING,
TS_MYA_FECHA_MODIFICACION               STRING,
DS_MYA_OPERADOR                         STRING
)
PARTITIONED BY (PARTITION_DATE STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/mya/fact/bt_mya_subscripciones'