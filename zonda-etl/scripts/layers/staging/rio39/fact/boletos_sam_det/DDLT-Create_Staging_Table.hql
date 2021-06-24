CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.comex_rio39_boletos_sam_det(
  ID_BOL                    STRING,
  IMPORTE                   STRING,
  CONCEPTOBCRA              STRING,
  PERMISO_EMBARQUE          STRING,
  ID_DET                    STRING,
  FECHA_EMBARQUE            STRING,
  ACUMULA_SALDO             STRING,
  DJAI                      STRING,
  DJAI_EXCEP                STRING,
  TIPO_DEC                  STRING,
  COD_OBSERVACION           STRING,
  OBSERVACION               STRING,
  FECHA_VENC                STRING,
  GESTION_COBRO             STRING,
  BS_POS_ARANC              STRING
 )
PARTITIONED BY(PARTITION_DATE STRING)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio39/fact/boletos_sam_det';