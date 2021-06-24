CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_cc_retresultado(

COD_CC_OPERACION                            STRING,
COD_CC_ENCUESTA                             STRING,
DT_CC_FECHA                                 STRING,
COD_PER_NUP                                 BIGINT,
COD_PER_NUPTITULAR                          BIGINT,
COD_CC_LEGAJO                               STRING,
DS_CC_NUMDOC                                BIGINT,
COD_CC_SUCURSAL                             INT,
COD_CC_CUENTA                               BIGINT,
DS_CC_COMENTARIO                            STRING,
COD_CC_NROVISA                               BIGINT,
COD_CC_CUENTAVISA                           BIGINT,
COD_CC_NROAMEX                               BIGINT,
COD_CC_CUENTAAMEX                           BIGINT,
COD_CC_NROMASTER                             BIGINT,
COD_CC_CUENTAMASTER                         BIGINT

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
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/cc/fact/bt_cc_retresultado'
