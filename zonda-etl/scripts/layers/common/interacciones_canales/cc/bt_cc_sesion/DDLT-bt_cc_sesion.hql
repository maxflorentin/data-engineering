CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.bt_cc_sesion(

COD_CC_SESION                           STRING,
COD_CC_OPERADORA                        STRING,
COD_CC_CLAVE                            STRING,
COD_CC_TIPODOC                          STRING,
DS_CC_NUMDOC                            BIGINT,
DS_CC_RAZONSOCIAL                       STRING,
DS_CC_CANTPRODUCTOS                     STRING,
DS_CC_TIPOPERSONA                       STRING,
DS_CC_CANTFAXPENDIENTES                 STRING,
FLAG_CC_FIRMANTESESION                  INT,
FLAG_CC_INDSINONIMO                     INT,
DS_CC_MDLWOK                            STRING,
DS_CC_NODOARBOLIVR                      STRING,
TS_CC_FECHA                             STRING,
DS_CC_NOMBRE                            STRING,
DS_CC_PRIMERAPELLIDO                    STRING,
DS_CC_SEGUNDOAPELLIDO                   STRING,
COD_PER_NUP                             BIGINT,
DS_CC_IDRACF                            STRING,
DS_CC_TIPOPUBLICIDAD                    STRING,
FLAG_CC_MARCAIP                         INT,
FALG_CC_MARCAANPH                       INT,
DS_CC_SEMAFOROFACTURACION               STRING,
DS_CC_SEMAFORORENTABILIDAD              STRING,
DS_CC_SEMAFORORIESGO                    STRING,
COD_CC_CONTACTO                         STRING,
COD_CC_SERVICIO                         STRING,
COD_CC_CONVERSACION                     STRING,
DS_CC_CATCLIENTE                        STRING,
DS_CC_ESMONOPRODUCTO                    STRING,
DS_CC_TIPOCLAVE                         STRING,
FLAG_CC_HORARIOMORA                     INT,
FLAG_CC_ABANDONO_IVR                    INT,
DS_CC_ABANDONO                          STRING

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
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/cc/fact/bt_cc_sesion'
