CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.comex_rio39_paises(
COD_BCRA                       STRING,
DESCRIPCION                    STRING,
CUIT_PERSONA_FISICA            STRING,
CUIT_PERSONA_JURIDICA          STRING,
AFIP                           STRING,
CONVENIO                       STRING,
ALERTA                         STRING,
CON_ACRED                      STRING,
PROHIBIDO                      STRING,
FECHA_BAJA_PAIS                STRING,
USU_MOD                        STRING,
FECHA_MOD                      STRING
)
PARTITIONED BY (partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio39/dim/paises';