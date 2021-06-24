CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.comex_rio39_opp_ops_pagos_det(
  COD_OPS                 STRING,
  SECUENCIA               STRING,
  CONCEPTO                STRING,
  DBTO_CDTO               STRING,
  PORCENTAJE              STRING,
  COD_MONEDA              STRING,
  COTIZACION              STRING,
  IMPORTE_ORIGEN          STRING,
  IMPORTE_PESOS           STRING,
  MOVIM_CTA_REF           STRING,
  SEC_MOV_CTA             STRING,
  IMPORTE_DOLAR           STRING
)
PARTITIONED BY(PARTITION_DATE STRING)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio39/fact/opp_ops_pagos_det';