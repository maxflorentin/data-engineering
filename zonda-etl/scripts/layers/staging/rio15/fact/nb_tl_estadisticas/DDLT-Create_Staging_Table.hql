CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio15_nb_tl_estadisticas
(
FECHA              STRING,
CUIT               STRING,
TRANSACCION        STRING,
COD_ERROR          STRING,
PID                STRING,
TIPO               STRING,
COMPROBANTE        STRING,
IMPORTE            STRING,
MONEDA             STRING,
TIPO_CUENTA        STRING,
COD_SUCURSAL       STRING,
ESTADO             STRING,
ID_SOLICITUD       STRING,
NRO_CUENTA         STRING,
TIPO_CUENTA_CRD    STRING,
NRO_CUENTA_CRD     STRING,
COD_SUCURSAL_CRD   STRING,
F_ALTA_REGISTRO    STRING,
TIPO_EMPRESA       STRING,
DESTINATARIO       STRING,
TIPO_TECLADO       STRING,
NUP                STRING,
RPI_MEDIO_PAGO     STRING,
RPI_CUITEMPSEV     STRING,
RPI_ID_SERVICIO    STRING,
RPI_NOMFANTASIA    STRING,
RPI_IDENTCLIENTEEMP STRING
)
PARTITIONED BY (
  partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio15/fact/nb_tl_estadisticas/'