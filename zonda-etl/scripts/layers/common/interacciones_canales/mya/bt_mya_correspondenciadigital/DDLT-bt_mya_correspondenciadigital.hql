CREATE EXTERNAL TABLE IF NOT EXISTS BI_CORP_COMMON.BT_MYA_CORRESPONDENCIADIGITAL (
ID_MYA_CORRESPONDENCIA                  STRING,
COD_MYA_MENSAJE                         STRING,
DS_MYA_TIPO_CARTA                       STRING,
DS_MYA_NOMBRE_APELLIDO                  STRING,
DS_MYA_RAZON_SOCIAL                     STRING,
DS_MYA_TELEFONO                         STRING,
COD_MYA_TIPO_DOCUMENTO                  STRING,
DS_MYA_TIPO_DOCUMENTO                   STRING,
COD_MYA_NUMDOC                          BIGINT,
DS_MYA_VALIDA_DIRECCION                 STRING,
DS_MYA_CALLE                            STRING,
DS_MYA_NUMEROD                          STRING,
DS_MYA_PISO                             STRING,
DS_MYA_DEPTO                            STRING,
DS_MYA_BARRIO                           STRING,
DS_MYA_LOCALIDAD                        STRING,
DS_MYA_CODIGOPOSTAL                     STRING,
COD_MYA_PROVINCIA                       STRING,
DS_MYA_PROVINCIA                        STRING,
COD_MYA_PAIS                            STRING,
DS_MYA_PAIS                             STRING,
DS_MYA_LUGAR_FECHA                      STRING,
COD_PER_NUP                             BIGINT,
DS_MYA_MARCA_TARJETA                    STRING,
DS_MYA_TIPO_PRESTAMOS                   STRING,
DS_MYA_NUM_TARJETA                      STRING,
COD_MYA_SUCURSAL                        INT,
DS_MYA_SUCURSAL_NOMBRE                  STRING,
DS_MYA_SUCURSAL_DOMICILIO               STRING,
FC_MYA_MONTO                            DECIMAL(19,4),
COD_MYA_DIVISA                          STRING,
DT_MYA_FECHA_CIERRE                     STRING,
INT_MYA_CANTIDAD_PRODUCTO               INT,
DS_MYA_PRODUCTO1                        STRING,
FC_MYA_MONTO1                           DECIMAL(19,4),
DS_MYA_PRODUCTO2                        STRING,
FC_MYA_MONTO2                           DECIMAL(19,4),
DS_MYA_PRODUCTO3                        STRING,
FC_MYA_MONTO3                           DECIMAL(19,4),
TS_MYA_FECHA_CREACION                   STRING,
TS_MYA_FECHA_MODIFICACION               STRING,
DS_MYA_TT_STS                           STRING,
TS_MYA_FECHA_IMPOSICION                 STRING,
DS_MYA_ULTIMO_RESULTADO                 STRING,
TS_MYA_FECHA_RESULTADO                  STRING

)
PARTITIONED BY (PARTITION_DATE STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/mya/fact/bt_mya_correspondeciadigial'