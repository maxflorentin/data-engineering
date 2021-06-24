CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.comex_rio39_config_conceptos (
ID_CONCEPTO       STRING,
COD_CONCEPTO      STRING,
DESC_CONCEPTO     STRING,
TIPO_CONCEPTO     STRING,
CON_DESP          STRING,
CON_NIF           STRING,
CON_IMPU          STRING,
CON_FEMB          STRING,
ACTIVO            STRING,
CON_4834          STRING,
MONTO_4834        STRING,
CON_EMPVI         STRING,
MONTO_EMPVI       STRING,
COD_DDJJ          STRING,
AYU_DOCU          STRING,
CON_FEMB_EST      STRING,
USU_MODIF         STRING,
FECHA_MODIF       STRING,
COD_CANAL         STRING,
COD_PRODUCTO      STRING,
CON_4237          STRING,
CON_NIF_GAN       STRING,
INV_ACRE          STRING,
CON_POS_ARANC     STRING,
EDITA_IMPO        STRING,
CON_TRF           STRING,
CON_5751          STRING,
CON_CUIT_BENEF    STRING,
CON_FORM_INV      STRING,
MONEDA_CUENTA     STRING,
AVANZA_VINCULADA  STRING
)
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio39/dim/config_conceptos';