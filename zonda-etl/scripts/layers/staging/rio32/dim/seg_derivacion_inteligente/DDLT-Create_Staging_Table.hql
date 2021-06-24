CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio32_seg_derivacion_inteligente(
TIPO_VDN_ENTRADA                            STRING,
APLICACION                                  STRING,
TIPO_RENTA                                  STRING,
NOMBRE_PRODUCTO                             STRING,
OPCION_IVR                                  STRING,
TIPO_PERSONA                                STRING,
NOMBRE_VDN_SALIDA                           STRING,
NOMBRE_VQ_SALIDA                            STRING,
GRUPO_AGENTES                               STRING,
GRUPO_AGENTES_DESBORDE                      STRING,
NOMBRE_VQ_ESTADISTICA                       STRING,
HABILIDAD_REQ_1                             STRING,
HABILIDAD_REQ_2                             STRING,
COD_CARTERA                                 STRING
)

PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio32/dim/seg_derivacion_inteligente';