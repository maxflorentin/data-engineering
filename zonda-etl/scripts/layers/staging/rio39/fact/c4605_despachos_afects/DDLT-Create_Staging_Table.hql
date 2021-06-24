CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.comex_rio39_c4605_despachos_afects(
  NRO_DESPACHO                      STRING,
  COD_OPS                           STRING,
  NRO_SEQUENCIA                     STRING,
  IMPORTE_MONEDA_OPERACION          STRING,
  IMPORTE_MONEDA_DESPACHO           STRING,
  COTIZACION_PASE                   STRING,
  FECHA_AFECT                       STRING,
  OBSERVACIONES                     STRING,
  USU_ALTA                          STRING,
  USU_MODIF                         STRING,
  FECHA_MODIF                       STRING,
  USU_BAJA                          STRING,
  FECHA_BAJA                        STRING,
  USU_VERIF                         STRING,
  FECHA_VERIF                       STRING,
  FECHA_TOMADO                      STRING,
  SEC_BCRA_DESP                     STRING,
  SEC_BCRA_ANT                      STRING,
  INFORME_ALTA                      STRING,
  FECHA_INFORME_ALTA                STRING,
  INFORME_BAJA                      STRING,
  FECHA_INFORME_BAJA                STRING,
  COD_TIPO                          STRING,
  ID_DET                            STRING,
  MONTO_C5060                       STRING,
  TIPO_CERTIFICACION                STRING,
  TIPO_REPORTE                      STRING,
  SEC_AFECT                         STRING,
  POS_ARANC                         STRING)
PARTITIONED BY(fecha_alta string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio39/fact/c4605_despachos_afects';