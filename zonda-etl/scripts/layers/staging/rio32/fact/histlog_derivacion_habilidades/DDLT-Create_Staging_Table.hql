CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio32_histlog_derivacion_habilidades(
FECHA                           STRING,
NUP                             STRING,
DOCUMENTO                       STRING,
CONNECTIONID                    STRING,
VQ                              STRING,
GRUPO_AGENTES                   STRING,
GRUPO_AGENTES_DES               STRING,
HABILIDAD_1                     STRING,
HABILIDAD_2                     STRING,
AGENTE                          STRING,
DESCRIP                         STRING,
COD_APP                         STRING,
SESION                          STRING
)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio32/fact/histlog_derivacion_habilidades';