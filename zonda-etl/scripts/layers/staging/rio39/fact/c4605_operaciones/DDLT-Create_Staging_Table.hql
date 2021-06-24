CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.comex_rio39_c4605_operaciones(
  COD_OPS                        STRING,
  ORDENANTE                      STRING,
  TIPO_DOC                       STRING,
  NRO_DOC                        STRING,
  NRO_BANKTRADE                  STRING,
  IMPORTE                        STRING,
  MONCOD                         STRING,
  FECHA_LIQ                      STRING,
  CONCEPTO_BCRA                  STRING,
  NRO_BOLETO                     STRING,
  FECHA_VENC                     STRING,
  ESTADO                         STRING,
  DENUNCIADO                     STRING,
  FECHA_DENU                     STRING,
  ENVIADO_DENU                   STRING,
  REGULARIZADO                   STRING,
  FECHA_REGU                     STRING,
  ENVIADO_REGU                   STRING,
  OBSERVACIONES                  STRING,
  USU_ALTA                       STRING,
  USU_MODIF                      STRING,
  FECHA_MODIF                    STRING,
  USU_BAJA                       STRING,
  FECHA_BAJA                     STRING,
  USU_VERIF                      STRING,
  FECHA_VERIF                    STRING,
  FECHA_PARA_DENU                STRING,
  RANGO_COMI                     DECIMAL,
  RANGO_MONTO                    STRING,
  VALIDADOBCRA                   STRING,
  ENSEGUIMIENTO                  STRING,
  FECHA_VALIDACION_BCRA          STRING,
  FECHA_TOMADO                   STRING)
PARTITIONED BY(fecha_alta string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio39/fact/c4605_operaciones';