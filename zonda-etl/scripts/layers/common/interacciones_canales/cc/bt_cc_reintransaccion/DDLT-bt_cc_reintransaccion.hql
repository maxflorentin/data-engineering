CREATE EXTERNAL TABLE IF NOT EXISTS  bi_corp_common.bt_cc_reintransaccion(
COD_CC_HEADERID                                 STRING,
COD_CC_SESION                                   STRING,
COD_CC_NOMBRE                                   STRING,
DS_CC_NOMBRE                                    STRING,
DS_CC_VERSION                                   STRING,
DS_CC_NUMERO                                    STRING,
TS_CC_FECHAINICIO                               STRING,
TS_CC_FECHAFINALIZACION                         STRING,
DS_CC_BACKEND                                   STRING,
DS_CC_USUARIO                                   STRING,
DS_CC_TERMINAL                                  STRING,
DS_CC_STATUS                                    STRING,
DS_CC_ESTADO                                    STRING,
FLAG_CC_REVERSIBLE                              INT,
FLAG_CC_REVERSA                                 INT,
FLAG_CC_REVERTIDA                               INT,
COD_CC_CANAL                                    STRING,
DS_CC_CANAL                                     STRING,
DS_CC_CANALVERSION                              STRING,
COD_CC_SUBCANAL                                 STRING,
COD_CC_DOCUMENTO                                STRING,
DS_CC_NUMDOC                                    BIGINT,
COD_CC_CUENTA_ORIGENTIPO                         STRING,
COD_CC_CUENTA_ORIGEN                        BIGINT,
COD_CC_SUCURSAL_CUENTAORIGEN                      INT,
COD_CC_CUENTA_DESTINOTIPO                        STRING,
COD_CC_CUENTA_DESTINO                      BIGINT,
COD_CC_SUCURSAL_CUENTADESTINO                     INT,
FC_CC_IMPORTE                                   DECIMAL(19,4),
DS_CC_TIPOCLIENTE                               STRING,
DS_CC_BIMONETARIA                               STRING,
FLAG_CC_AGENDADA                                INT,
DS_CC_CANALTIPO                                 STRING,
DS_CC_SUBCANALTIPO                              STRING,
DS_CC_IDSECUNDARIO                              STRING,
DS_CC_SERVICIOPAGADO                            STRING,
FLAG_CC_SIMULACION                              INT,
FLAG_CC_IDENTIFICACIONPIN                       INT,
FLG_CC_VERIFICACION                             INT,
FLAG_CC_GENUINO                                 INT,
DS_CC_APLICACION                                STRING
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/cc/fact/bt_cc_reintransaccion'