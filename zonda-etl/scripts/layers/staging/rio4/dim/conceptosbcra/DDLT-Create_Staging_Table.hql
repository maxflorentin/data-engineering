CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio4_conceptosbcra(
CODIGO                          STRING,
DESCRIPCION                     STRING,
ALIAS1                          STRING,
ALIAS2                          STRING,
ALIAS3                          STRING,
EXTENDIDO                       STRING,
IMPUESTO                        STRING,
TASA                            STRING,
FECHA_ALTA                      STRING,
FECHA_BAJA                      STRING,
UTILIZA_CUOTA                   STRING,
COD_AFIP                        STRING,
VALIDA_AFIP                     STRING,
MULTIPLE_DEJAI                  STRING,
TIPO_REFERENCIA_AFIP            STRING,
TIPO_REGISTRACION_AFIP          STRING,
PERMISO_EMBARQUE                STRING,
FECHA_EMBARQUE                  STRING,
TURISMO                         STRING,
ARCHIVO_TURISMO                 STRING,
GRUPO                           STRING,
GRUPOM                          STRING,
RG3421                          STRING,
CXD                             STRING,
INFBCRA                         STRING
)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio4/dim/conceptosbcra';