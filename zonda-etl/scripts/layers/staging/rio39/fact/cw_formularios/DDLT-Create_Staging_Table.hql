CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.comex_rio39_cw_formularios(
  NRO_FORM                            STRING,
  TIPO_FORM                           STRING,
  SUCURSAL                            STRING,
  ACCT_NO                             STRING,
  CUIT                                STRING,
  COD_CONCEP                          STRING,
  MONEDA                              STRING,
  MONTO                               STRING,
  BANCO_RECEPT                        STRING,
  BANCO_DOMIC                         STRING,
  BANCO_INTERM                        STRING,
  BENEFICIARIO                        STRING,
  BENEF_DOMIC                         STRING,
  BENEF_CTA                           STRING,
  OBSERVACIONES                       STRING,
  NRO_OPER_REL                        STRING,
  FECHA_FACT                          STRING,
  IVA                                 STRING,
  MOTIVO_IVA                          STRING,
  GANANCIAS                           STRING,
  MOTIVO_GAN                          STRING,
  ACARGO_GAN                          STRING,
  ESTADO                              STRING,
  MOTIVO_RECHAZO                      STRING,
  USER_CARGA                          STRING,
  USER_VERIF                          STRING,
  FECHA_VERIF                         STRING,
  USER_APROB                          STRING,
  FECHA_APROB                         STRING,
  TIPO_CIERRE                         STRING,
  TIPO_OPCALZ                         STRING,
  OP_PARCIAL                          STRING,
  DOBLE_IMPOSICION                    STRING,
  ALICUOTA_IVA                        STRING,
  ALICUOTA_GAN                        STRING,
  MONTO_FOB                           STRING,
  MONTO_FLETE                         STRING,
  MONTO_SEG                           STRING,
  ACREEDOR                            STRING,
  TASA                                STRING,
  PLAZO                               STRING,
  C_NOMBRE                            STRING,
  FECHA_GUIA                          STRING,
  POSICION                            STRING,
  VER_FORM                            STRING,
  TIENE_ADJUNTOS                      STRING,
  LOG_IMAGENES                        STRING,
  CANAL                               STRING,
  PAIS_ORIGEN                         STRING,
  PAIS_BENEF                          STRING,
  BCO_PAGADOR_CORRE                   STRING,
  DESC_GAN                            STRING,
  BASEIMPO_IVA                        STRING,
  BASEIMPO_GAN                        STRING,
  FECHA_SWIFT                         STRING,
  BCO_CORRE_AKON                      STRING,
  COD_OPS                             STRING,
  LIQ_BKT                             STRING,
  SECUENCIA                           STRING,
  INT_CTA                             STRING,
  TIPO_DOC                            STRING,
  NRO_DOC                             STRING,
  TIPO_PERSONA                        STRING,
  TIPO_IVA                            STRING,
  TIPO_CLIENTE                        STRING,
  EMPLEADO                            STRING,
  NUP                                 STRING,
  ES_FACTURA_E                        STRING,
  COD_DDJJ                            STRING,
  RESP_CLIE                           STRING,
  REF_CLIENTE                         STRING,
  CARGO_GASTOS                        STRING,
  INTERM_CTA                          STRING,
  INST_VENDIDO                        STRING,
  INST_RECIBIDO                       STRING,
  FECHA_EMB                           STRING,
  DDJJ_ADIC                           STRING,
  DDJJ_OK                             STRING,
  APLICA_GAN                          STRING,
  DOBLE_IMP_PAIS                      STRING,
  DOBLE_IMP_ART                       STRING,
  INPI                                STRING,
  F_INTEREST                          STRING,
  F_INTEREST_OPCION                   STRING,
  F_INTEREST_OTRO                     STRING,
  F_INTEREST_NO                       STRING,
  NO_RET_OPCION                       STRING,
  NO_RET_ART14                        STRING,
  NO_RET_MOTIVO                       STRING,
  NO_RET_PAIS                         STRING,
  NO_RET_ARTICULO                     STRING,
  APLICA_IVA                          STRING,
  NO_APLICA_IVA_OP                    STRING,
  OTROS_NO_IVA                        STRING,
  COM_4834_OP                         STRING,
  COM_4834_TEXTO                      STRING,
  EMPVI_OP1                           STRING,
  EMPVI_OP2                           STRING,
  EMPVI_OP3                           STRING,
  EMPVI_OP4                           STRING,
  EMPVI_OP5                           STRING,
  NRO_SOLICITUD                       STRING,
  TIPO_TRF                            STRING,
  COM_RECHAZO                         STRING,
  VIGENTE                             STRING,
  MONTO_RET_IVA                       STRING,
  MONTO_RET_GAN                       STRING,
  ID_CT                               STRING,
  CTA_ALTAIR_CLIENTE                  STRING,
  FECHA_EMB_EST                       STRING,
  ID_ALICUOTA                         STRING,
  ID_COND_VTA                         STRING,
  RAZON_SOCIAL                        STRING,
  ESTADO_EJECUCION                    STRING,
  USUARIO_EJECUCION                   STRING,
  USUARIO_ASIGNADO                    STRING,
  FECHA_ASIGNADO                      STRING,
  SUBSEGMENTO                         STRING,
  NRO_FORM_REL                        STRING,
  CORREGIDA                           STRING,
  VINCULO                             STRING,
  NOM_BENEF                           STRING,
  COD_DESTINO                         STRING,
  CUADRANTE                           STRING,
  CON_5751                            STRING,
  OP_5751                             STRING,
  FECHA_FIRMA                         STRING,
  AUTOGESTION                         STRING,
  ACEPTA_DDJJ                         STRING,
  ACEPTA_DDJJ_FI                      STRING,
  ASOLA_FIRMA                         STRING,
  CANCELA_INTERESES                   STRING,
  CAPITAL_A_CANCELAR                  STRING,
  CAPITAL_A_PRORROGAR                 STRING,
  CESION_GARANTIA                     STRING,
  COD_CONCEP_PENALTY                  STRING,
  COD_ESTADO                          STRING,
  COD_MOT_RECHAZO                     STRING,
  COD_PRODUCTO                        STRING,
  FECHA_VENCIMIENTO                   STRING,
  FIANZA_ESPECIFICA                   STRING,
  FIANZA_GENERICA                     STRING,
  HIPOTECA                            STRING,
  ID_GRUPO_BOLETO                     STRING,
  IMPORTE_PRESTAMO                    STRING,
  INSTANCIA                           STRING,
  INTERESES                           STRING,
  METODO_CANCELACION                  STRING,
  MODALIDAD                           STRING,
  MONTO_FIJO                          STRING,
  MONTO_PENALTY                       STRING,
  NRO_LIQ_SGE                         STRING,
  NRO_PRESTAMO_REL                    STRING,
  NRO_SECUENCIA_BKT                   STRING,
  NRO_SECUENCIA_PRESTAMO_BKT          STRING,
  OBSERVACIONES_GTIA                  STRING,
  OTROS                               STRING,
  PENALTY                             STRING,
  PORC_COMISION                       STRING,
  PRENDA_FIJA_FLOT                    STRING,
  PRENDA_REGISTRO                     STRING,
  STANDBY                             STRING,
  TASA_CFT                            STRING,
  TASA_TNA                            STRING,
  TIPO_AMORTIZACION                   STRING,
  TIPO_DOC_CLI                        STRING,
  TIPO_OPERACION                      STRING,
  TIPO_PRESTAMO                       STRING,
  TOTAL_A_CANCELAR                    STRING,
  TOTAL_CANC_DEBITO_CTA               STRING,
  TOTAL_CANC_ORD_PAGO                 STRING,
  ID_AGENDA                           STRING,
  ID_NIF                              STRING,
  INV_ACRE                            STRING,
  NIF_GAN                             STRING,
  CUIT_BENEF                          STRING,
  OP_GAN                              STRING,
  MOTIVO_RECHAZO_PCC                  STRING,
  NOMBRE_OFICIAL_COMERCIAL            STRING,
  SALDO_PMO_ORIGEN_CANC               STRING,
  TASA_TEA                            STRING,
  APELLIDO_OFICIAL_COMERCIAL          STRING,
  MAIL_OFICIAL_COMERCIAL              STRING,
  USERCONTROL                         STRING,
  TIMESTAMP                           STRING,
  RUBRO_BENEF                         STRING,
  GENERA_COMPROB                      STRING,
  MAIL_SUC                            STRING,
  NRO_FORM_INVERSION                  STRING,
  CARGO_GASTOS_IMPORTE                STRING,
  TIPO_BANCO_RECEPTOR                 STRING,
  TIPO_BANCO_INTERMEDIARIO            STRING,
  USU_RETRO                           STRING,
  FECHA_RETRO                         STRING,
  FECHA_ORIG_RECHAZO                  STRING,
  USU_ORIG_RECHAZO                    STRING,
  NRO_BOLETO_DEV                      STRING,
  FECHA_CARGA                         STRING,
  ID_CELULA                           STRING
)
PARTITIONED BY (PARTITION_DATE STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio39/fact/cw_formularios';