CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_cc_retdetalleresultado(
COD_CC_OPERACION                                    STRING,
COD_PER_NUP											BIGINT,
COD_CC_ENCUESTA                                     STRING,
DS_CC_ENCUESTA                                      STRING,
FLAG_CC_ENCUESTA_ACTIVA                             INT,
DS_CC_ENCUESTA_TIPO                                 STRING,
COD_CC_PRODUCTO                                     STRING,
DS_CC_PRODUCTO                                      STRING,
COD_CC_PREGUNTA                                     STRING,
DS_CC_PREGUNTA                                      STRING,
FLAG_CC_RESPUESTA_MULTIPLE                          INT,
FLAG_CC_PREGUNTA_ACTIVA                             INT,
COD_CC_RESPUESTA                                    STRING,
DS_CC_RESPUESTA                                     STRING,
FLAG_CC_RESPUESTA_ACTIVA                            INT,
DS_CC_COMENTARIO                                    STRING
)
PARTITIONED BY (
    partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/cc/fact/bt_cc_retdetalleresultado'


