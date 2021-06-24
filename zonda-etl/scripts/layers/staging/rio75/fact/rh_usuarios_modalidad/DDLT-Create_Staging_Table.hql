CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio75_rh_usuarios_modalidad(
N_UMOD_ID                               STRING,
C_USUARIORH                             STRING,
C_EMPRESA                               STRING,
C_TIPO_CONTRATO                         STRING,
C_UBICACION                             STRING,
F_ALTA                                  STRING,
F_BAJA                                  STRING,
I_ACTIVO                                STRING
)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '/santander/bi-corp/staging/rio75/fact/rh_usuarios_modalidad';