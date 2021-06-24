CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.comex_rio39_comcob(
  COMNUM                   STRING,
  TIPO_COM                 STRING,
  NRO_DESTINACION          STRING,
  CUIT                     STRING,
  CUENTA_OPER              STRING,
  MONTO_USD                STRING,
  ACCOUNTNUM               STRING,
  COTIZA                   STRING,
  MONTO_ARG                STRING,
  IVA1                     STRING,
  TIPO_IVA2                STRING,
  IVA2                     STRING,
  NETO                     STRING,
  FECHA_PROC               STRING,
  FECHA_VALOR              STRING,
  ESTADO                   STRING,
  CONTAB                   STRING,
  COMPROB                  STRING,
  OBSERVACIONES            STRING,
  LEYENDACOMPROB           STRING,
  CARGA_USU                STRING,
  VERIF_USU                STRING,
  VERIF_FECHA              STRING,
  INFNUM_INF_BCRA          STRING,
  FECHA_PRES_3602          STRING,
  COD_AGRUPA               STRING,
  COD_3602                 STRING,
  COD_4237                 STRING,
  ID_CID                   STRING,
  ID_CID_OP                STRING,
  CORRIDA                  STRING,
  COB_DEV                  STRING,
  MARCA_IVA                STRING
)
PARTITIONED BY(CARGA_FECHA string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio39/fact/comcob';