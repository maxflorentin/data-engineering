CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio75_rh_usuarios(
C_USUARIORH                         STRING,
C_USUARIONT                         STRING,
C_USUARIO_RIO3                      STRING,
N_LEGAJO_BUP                        STRING,
N_LEG_ADMIN                         STRING,
D_NOMBRE                            STRING,
D_APELLIDO                          STRING,
D_CUIT_CUIL                         STRING,
T_DOCUMENTO                         STRING,
N_DOCUMENTO                         STRING,
F_NACIMIENTO                        STRING,
I_SEXO                              STRING,
D_TELEFONO1                         STRING,
D_TELEFONO2                         STRING,
D_INTERNO                           STRING,
D_EMAIL                             STRING,
D_CALLE                             STRING,
D_NUMERO                            STRING,
D_PISO                              STRING,
D_DEPTO                             STRING,
D_LOCALIDAD                         STRING,
D_COD_POSTAL                        STRING,
D_PROVINCIA                         STRING,
D_TIPO_EMPLEADO                     STRING,
V_HORA_IN                           STRING,
V_HORA_OUT                          STRING,
F_INGRESO                           STRING,
F_EGRESO                            STRING,
D_ESTADO_BUP                        STRING,
D_CCOSTO                            STRING,
C_MOTIVO_BAJA                       STRING,
C_USU_BKP                           STRING,
F_CARGA_BAJA                        STRING,
N_USUARIO_RACF                      STRING,
C_ID_TELEFONICO                     STRING

)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/santander/bi-corp/staging/rio75/fact/rh_usuarios';