CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.comex_rio39_estadistica_wabkt150_d(
FRECUENCIA			STRING,
PROD_CTBLE          STRING,
SUBPRODUCTO         STRING,
TIPOINFO            STRING,
FECHA_ALTA          STRING,
FECHA_BAJA          STRING,
FECHA_VTO           STRING,
ESTADO              STRING,
SUC_OPERAC          STRING,
CTA_OPERAC          STRING,
DIVISA              STRING,
TIPO_CAMBIO         STRING,
TIPO_CAMBIO_OPERAC  STRING,
SUC_TRAN            STRING,
CANAL_TRAN          STRING,
NRO_OPERAC          STRING,
IMP_OPERAC          STRING,
IMP_ALTA_OPERAC     STRING,
NUP                 STRING,
ENTIDAD_JURID       STRING,
PAIS                STRING,
FECHA_PROC          STRING,
BANCO_CORRE         STRING,
ORDENANTE           STRING,
FECHA_PROCESO       STRING,
CIUDAD              STRING,
ORIGEN              STRING,
DOM_BANCO_CORRE     STRING,
PAIS_ORDENANTE      STRING,
DOM_ORDENANTE       STRING,
BCO_CORR_RIO        STRING,
IMP_OPERAC_ARS      STRING,
NUP_TITULAR         STRING,
CONCEPTOBCRA        STRING,
CUENTA_EN           STRING
)
PARTITIONED BY (PARTITION_DATE STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio39/fact/estadistica_wabkt150_d';