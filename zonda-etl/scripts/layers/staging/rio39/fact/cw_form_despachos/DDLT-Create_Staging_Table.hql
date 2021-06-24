CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.comex_rio39_cw_form_despachos(
  NRO_FORM                  STRING,
  SECUENCIA                 STRING,
  NRO_DOC                   STRING,
  IMPORTE                   STRING,
  IMP_APLIC                 STRING,
  BANCO                     STRING,
  TIPO_DECLARACION          STRING,
  NRO_DJ                    STRING,
  MOT_EXCEP                 STRING,
  COD_MONEDA                STRING,
  CARGA_MANUAL              STRING,
  POSICION_ARANC            STRING,
  ESPERA_CERTIF             STRING,
  FECHA_EMBARQUE            STRING
)
PARTITIONED BY (PARTITION_DATE STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio39/fact/cw_form_despachos';