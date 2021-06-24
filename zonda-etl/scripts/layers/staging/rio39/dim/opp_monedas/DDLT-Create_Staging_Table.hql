CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.comex_rio39_opp_monedas(
MONCOD                STRING,
MONDES                STRING,
COD_AFIP              STRING,
COD_BCRA              STRING,
USU_ALTA              STRING,
FECHA_ALTA            STRING,
USU_MODIF             STRING,
FECHA_MODIF           STRING,
USU_BAJA              STRING,
FECHA_BAJA            STRING,
USU_VERIF             STRING,
FECHA_VERIF           STRING,
CORRESPONSAL          STRING,
OPER_SML              STRING,
GPI                   STRING
)
PARTITIONED BY(partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio39/dim/opp_monedas';