CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio147_hb_creditos_preaprobados_mono(
ID                  STRING,
NUP                 STRING,
FECHA_INICIO_VIG    STRING,
FECHA_FIN_VIG       STRING,
DIVISA              STRING,
CANT_CUOTAS_MIN     STRING,
CANT_CUOTAS_MAX     STRING,
IMPORTE_MIN         STRING,
IMPORTE_MAX         STRING,
TIPO_TASA           STRING,
TNA                 STRING,
CFTA_SIN_IMP        STRING,
CFTA_CON_IMP        STRING,
ENTIDAD_UG          STRING,
PRODUCTO_UG         STRING,
SUBPRODUCTO_UG      STRING,
HABILITADO          STRING,
SOLICITADO          STRING,
FECHA_ALTA_REGISTRO STRING
)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio147/fact/hb_creditos_preaprobados_mono';