CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.gsa_v_especies(
OID_ESPECIE                        STRING,
SECID                              STRING,
TIPO_ESPECIE              		   STRING,
DENOMINACION                       STRING,
COD_ROSSI                          STRING,
COD_BCE                            STRING,
COD_ISIN                           STRING,
COD_SAM                            STRING,
COD_BCRA                           STRING,
COD_CVSA                           STRING,
COD_ATIT                           STRING,
COD_MAE                            STRING,
COD_FM                             STRING,
COD_AMIGO                          STRING,
PAIS_EMISOR                        STRING,
MONEDA_DE_EMISION                  STRING,
FECHA_CUP_PROX                     STRING,
FECHA_CUP_ANT                      STRING,
FECHA_VTO                          STRING,
PERIODICIDAD_PAGOS                 STRING,
TIPO_TASA                          STRING,
TASA                               STRING,
VR                                 STRING,
LAM_MIN                            STRING,
MIN_INCRE                          STRING,
CASH_FLOW                          STRING,
TIPO_CAP_PRECIO                    STRING,
PRECIO_EJ                          STRING,
FECHA_EJ                           STRING,
PER_DE_LIQ                         STRING,
CANT_POR_LOTE                      STRING,
DIVIDEND_DATE                      STRING,
DIVIDEND                           STRING,
AMB_NEGOCIACION                    STRING,
OBSERVACIONES                      STRING,
ESTADO                             STRING,
SUBESPECIE                         STRING,
FECHA_PRECIO                       STRING,
PLAZO                              STRING,
TITULO_PUBLICO_O_PRIVADO           STRING,
LUGAR_DEFAULT_DE_CUSTODIA          STRING,
SIC                                STRING,
ESPECIE                            STRING,
FTE_DE_PRECIO                      STRING,
TIPO_VALORIZACION                  STRING,
MONEDA_DE_COTIZACION               STRING,
CLASIFICACION_EXTRACTO             STRING,
BASIS                              STRING,
UNSOLICITED                        STRING,
PRODTYPE                           STRING,
INTERESES_CORRIDOS                 STRING,
PRODUCTO                           STRING,
COD_CATEGORIA_CV                   STRING,
OBRA_PUBLICA                       STRING,
FECHA_HASTA_AMB_NEG                STRING,
AJUSTE_DE_CAPITAL                  STRING,
INDICE                             STRING
)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/gsa/fact/v_especies';