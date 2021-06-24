CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_cc_ivropcionfinal(

TS_CC_FECHA                                 STRING,
COD_CC_SESION                               STRING,
COD_CC_VDN_ENTRADA                          STRING,
COD_CC_CODIGOAPP                            STRING,
DS_CC_OPCION_IVR                            STRING,
DS_CC_RECLAMO                               STRING,
FLAG_CC_SINIESTRO                           INT,
COD_CC_VDN_SALIDA                           STRING,
DS_CC_VQ                                    STRING,
COD_CC_GRUPO_AGENTES                        STRING,
DS_CC_GRUPO_AGENTES                         STRING,
DS_CC_VQ_ESTADISTICA                        STRING,
DS_CC_HABILIDAD1                            STRING,
DS_CC_HABILIDAD2                            STRING,
DS_CC_OPERADOR_TITCARTERA                   STRING,
DS_CC_ULTIMO_OPERADORSOLICITUD              STRING,
DS_CC_ULTIMO_GRUPORECLAMO                   STRING,
DS_CC_PUNTAJE_PRIORIDAD                     STRING,
DS_CC_PUNTAJE_ALERTAS                       STRING,
DS_CC_PUNTAJE_PFVENCIDO                     STRING,
DS_CC_TC_AVENCER                            STRING,
DS_CC_USUARIO_OLB                           STRING,
DS_CC_MORA                                  STRING,
DS_CC_CORREO                                STRING,
FLAG_CC_SOLICITUD                           INT,
DS_CC_CAMPANIA_ACTIVA                       STRING,
DS_CC_ENCUESTA_NEGATIVA                     STRING,
DS_CC_AUDIO                                 STRING,
DS_CC_ACCION                                STRING

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
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/cc/fact/bt_cc_ivropcionfinal'
