CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.comex_rio39_opp_ops(
  COD_OPS                      STRING,
  MONCOD                       STRING,
  COD_TIPO                     STRING,
  COD_DESTINO                  STRING,
  COD_ESTADO                   STRING,
  LIQ_BKT                      STRING,
  NRO_BANKTRADE                STRING,
  IMPORTE                      STRING,
  BENEFICIARIO                 STRING,
  TPOCTA                       STRING,
  SUCURSAL                     STRING,
  CUENTA                       STRING,
  NUMERO                       STRING,
  OBSERVA                      STRING,
  ORDENANTE                    STRING,
  FECHA_VALOR                  STRING,
  FECHA_MT                     STRING,
  ACCT_NO                      STRING,
  FECHA_LIQ                    STRING,
  FECHA_MODIF                  STRING,
  USUARIO                      STRING,
  ENVIO_MAIL                   STRING,
  F_ENVIO_MAIL                 STRING,
  NUEVO_RUBRO                  STRING,
  ASIENTO_CONTABLE             STRING,
  CON_FONDOS                   STRING,
  F_ASIENTO_CONTABLE           STRING,
  COD_PRODUCTO                 STRING,
  USUARIO_VERIFICA             STRING,
  FECHA_VERIFICA               STRING,
  VISTA                        STRING,
  PLAZO                        STRING,
  F_VENCIMIENTO                STRING,
  SUMA_PAGOS                   STRING,
  SALDO_PAGO                   STRING,
  LIQUIDA_PARCIALES            STRING,
  OPP_MAX_SEQ                  STRING,
  ESTADO_MAIL                  STRING,
  PAIS_ORIGEN                  STRING,
  PAIS_BENEF                   STRING,
  CUENTA_BP                    STRING,
  NUP                          STRING,
  COD_SWIFT_R                  STRING,
  DETALLE_SWIFT                STRING,
  MOTIVO_OBSERVA               STRING,
  TIPO_CTA_FONDO               STRING,
  NRO_FONDO                    STRING,
  TIPO_CLIENTE                 STRING,
  TIPO_DOC                     STRING,
  NRO_DOC                      STRING,
  TIPO_PERSONA                 STRING,
  TIPO_IVA                     STRING,
  EMPLEADO                     STRING,
  ID_SWIFT_CORRE               STRING,
  OBSERVA_ELIMINA              STRING,
  TIMESTAMP                    STRING,
  FECHA_VUELCO72               STRING,
  F_AVISO_SINLIQ               STRING,
  ES_COM_A5655                 STRING,
  COD_OPS_REL                  STRING,
  SECUENCIA_REL                STRING,
  COD_PROD_ALTAIR              STRING,
  ACREDIT_AUTOM                STRING,
  INFORMA_F2040                STRING,
  NRO_DOC_70                   STRING,
  NRO_LEGAJO_CITI              STRING,
  NRO_CUENTA_CITI              STRING,
  RAZON_SOCIAL_CITI            STRING,
  OP_CITY                      STRING,
  TIPO_DOC_CITY                STRING,
  NRO_DOC_CITY                 STRING,
  NRO_LEGAJO_CITI_ANT          STRING,
  DOMICILIO_ORD                STRING,
  ACREDIT_AUTOM_CTRL           STRING,
  CUADRANTE                    STRING,
  SUBSEGMENTO                  STRING,
  ID_ICS                       STRING,
  FECHA_NAC                    STRING,
  CAPITAL_CANCELADO            STRING,
  CAPITAL_PENDIENTE            STRING,
  INTERESES_BKT                STRING,
  ULT_SECUENCIA_BKT            STRING,
  ID_BOL_CA                    STRING,
  ID_BOL_NORMAL                STRING,
  ID_BOLETO_CITI               STRING,
  ID_SWIFT_GPII                STRING,
  SRP                          STRING,
  TASA_TNA                     STRING,
  TASA_CFT                     STRING,
  LIQ_BKT_PASIVO               STRING,
  TRF_ASOCIADAS                STRING,
  TASA_TEA                     STRING,
  GENERA_COMPROB               STRING,
  LOCALIDAD_ORD                STRING,
  SUBPROD_ALTAIR               STRING,
  ES_SML                       STRING,
  CONDICION_CORRE              STRING,
  FECHA                        STRING
)
PARTITIONED BY(PARTITION_DATE STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio39/fact/opp_ops';