CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_cc_ivrderivacionhabilidades(

TS_CC_FECHA                             STRING,
COD_PER_NUP                             BIGINT,
COD_CC_CONEXION                         STRING,
COD_CC_SESION                           STRING,
DS_CC_VQ                                STRING,
COD_CC_GRUPO_AGENTES                    STRING,
DS_CC_GRUPO_AGENTES                     STRING,
DS_CC_HABILIDAD1                        STRING,
DS_CC_HABILIDAD2                        STRING,
COD_CC_LEGAJO                           STRING,
DS_CC_DERIVACION                        STRING,
COD_CC_CODIGOAPP                        STRING

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
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/cc/fact/bt_cc_ivrderivacionhabilidades'
