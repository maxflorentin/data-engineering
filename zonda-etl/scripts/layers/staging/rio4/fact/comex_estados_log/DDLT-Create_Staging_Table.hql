CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio4_comex_estados_log(
NRO_PROPUESTA              STRING,
NRO_BOLETO                 STRING,
COTIZACION                 STRING,
ESTADO                     STRING,
PROCESADO                  STRING,
ID_LOG                     STRING,
FECHA_CARGA       		   STRING,
FECHA_PROCESO              STRING,
FECHA_LIQ_MON              STRING,
FECHA_LIQ_MON_EXT          STRING
)
PARTITIONED BY (FECHAOP string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio4/fact/comex_estados_log';