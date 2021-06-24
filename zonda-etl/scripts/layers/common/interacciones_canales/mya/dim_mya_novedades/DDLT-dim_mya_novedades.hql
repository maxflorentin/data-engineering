CREATE EXTERNAL TABLE IF NOT EXISTS BI_CORP_COMMON.DIM_MYA_NOVEDADES (
COD_MYA_NOVEDAD                         STRING,
DS_MYA_NOVEDAD                          STRING,
DS_MYA_DESCRIPCION                      STRING,
DS_MYA_DESCRIPCION_LARGA                STRING,
DS_MYA_LABEL                            INT,
DS_MYA_FRECUENCIA                       STRING,
DS_MYA_PERIODO_TIEMPO                   STRING,
INT_MYA_CANT_DIASENVIOS                 INT,
COD_MYA_ESTADO                          STRING,
FLAG_MYA_VISIBLE_FRONT                  INT,
FLAG_MYA_VISIBLE_BUSQUEDA               INT,
DS_MYA_DESCRIPCION_BUSQUEDA             STRING,
DS_MYA_PRIORIDAD                        STRING,
COD_MYA_SUBCANAL                        STRING,
COD_MYA_CANAL                           STRING,
COD_MYA_REGLA                           STRING,
COD_MYA_MODO_PROCESAMIENTO              STRING,
INT_MYA_TTL                             INT,
DS_MYA_PAGINA_MUESTRA                   STRING,
FLAG_MYA_NEWSLETTER                     INT,
COD_MYA_FILTRO                          STRING,
DS_MYA_ESPECIAL                         STRING,
FLAG_MYA_ACNL                           INT,
DS_MYA_URL                              STRING
)
PARTITIONED BY (PARTITION_DATE STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/mya/dim/dim_mya_novedades'