CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_cc_ivropciones(

TS_CC_FECHA                             STRING,
COD_CC_SESION                           STRING,
DS_CC_OPCION1                           STRING,
DS_CC_OPCION2                           STRING,
DS_CC_OPCION3                           STRING,
DS_CC_OPCION4                           STRING,
DS_CC_OPCION5                           STRING

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
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/cc/fact/bt_cc_ivropciones'
