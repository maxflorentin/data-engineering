CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.comex_rio39_c3493_oper_boletos(
  NRO_DESTINACION               STRING,
  NROPERACION                   STRING,
  NRO_SECUENCIA                 STRING,
  MONTO_OPERACION               STRING,
  FECHA_ENVIO                   STRING,
  ENVIADO                       STRING,
  FECHA_MODIF                   STRING,
  USUARIO                       STRING,
  FECHA_NEG                     STRING,
  FLAG                          STRING,
  ULT_NEG                       STRING,
  SECUENCIA                     STRING,
  NROOP                         STRING,
  NROSEQ                        STRING,
  TIPO_APLIC                    STRING,
  OBSERVACIONES                 STRING,
  IDCONCEPTOBOLTEC              STRING,
  USUARIO_ALTA                  STRING,
  ORIGEN                        STRING,
  COD_BANCO                     STRING,
  EMP_VINCULADAS                STRING,
  LIMITE_CAMBIO                 STRING,
  COTIZACION_PASE               STRING,
  MONTO_BOLETO                  STRING,
  ENVIO_KAE                     STRING,
  FECHA_ENVIO_KAE               STRING,
  NRO_SECUENCIA_KAE             STRING,
  CONCEPTO_KAE                  STRING,
  FECHA_ACCION_KAE              STRING,
  INFO_ADICIONAL_KAE_1          STRING,
  INFO_ADICIONAL_KAE_2          STRING,
  ID_MECANISMO                  STRING,
  ULT_INFNUM_KAE                STRING,
  M_SEC_KAE_MANUAL              STRING,
  FECHA_LIQ_ANTICIPO            STRING,
  CUMPLE_PARCIAL                STRING,
  FECHA_CANCEL_DEUDA            STRING,
  CONCEPTO_BOL                  STRING)
PARTITIONED BY(fecha_alta string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio39/fact/c3493_oper_boletos';