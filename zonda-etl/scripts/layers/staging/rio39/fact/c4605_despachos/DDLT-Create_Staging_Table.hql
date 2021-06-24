CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.comex_rio39_c4605_despachos(
  NRO_DESPACHO                   STRING,
  FECHA_EMB                      STRING,
  FECHA_NAC                      STRING,
  FECHA_OFICIALIZACION           STRING,
  POS_ARANC                      STRING,
  MONCOD                         STRING,
  IMPORTE                        STRING,
  MONTO_FOB                      STRING,
  MONTO_FLETES                   STRING,
  MONTO_SEGURO                   STRING,
  MONTO_AJUSTE                   STRING,
  MONTO_TOTAL                    STRING,
  TIPO_DOC                       STRING,
  NRO_DOC                        STRING,
  USU_ALTA                       STRING,
  USU_MODIF                      STRING,
  FECHA_MODIF                    STRING,
  USU_BAJA                       STRING,
  FECHA_BAJA                     STRING,
  USU_VERIF                      STRING,
  FECHA_VERIF                    STRING,
  FECHA_TOMADO                   STRING,
  MONTO_AJUSTE_DEDUCIR           STRING,
  ORIGEN                         STRING,
  COND_VTA                       STRING,
  COD_BANCO_EMI                  STRING,
  FK_ID_SWIFT_DESP               STRING,
  CERTIFICACION_CLIENTE          STRING)
PARTITIONED BY(fecha_alta string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio39/fact/c4605_despachos';