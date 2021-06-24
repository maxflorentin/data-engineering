CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio32_histdatosruteo_na(
FECHA                           STRING,
SESION                          STRING,
VDN_ENTRADA                     STRING,
CODIGOAPP                       STRING,
OPCION_IVR                      STRING,
ULTIMO_USUARIO_RECLAMO          STRING,
RECLAMO                         STRING,
SINIESTRO                       STRING,
VDN_SALIDA                      STRING,
VQ                              STRING,
GRUPO_AGENTES                   STRING,
GRUPO_AGENTES_DES               STRING,
VQ_ESTADISTICA                  STRING,
HABILIDAD_1                     STRING,
HABILIDAD_2                     STRING,
OPERADOR_TIT_CARTERA            STRING,
OPERADOR_SUP1_CARTERA           STRING,
OPERADOR_SUP2_CARTERA           STRING,
ULTIMO_OPERADOR_SOLICITUD       STRING,
ULTIMO_GRUPO_RECLAMO            STRING,
ULTIMO_GRUPO_SOLICITUD          STRING,
PUNTAJE_PRIORIDAD               STRING,
HABILIDADES_BONUS               STRING,
ALERTAS                         STRING,
PF_VENCIDO                      STRING,
TC_A_VENCER                     STRING,
USUARIO_OLB                     STRING,
MORA                            STRING,
CORREO                          STRING,
SOLICITUD                       STRING,
CAMPANIA_ACTIVA                 STRING,
ENCUESTA_NEGATIVA               STRING,
AUDIO                           STRING,
ACCION                          STRING
)

PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio32/fact/histdatosruteo_na';