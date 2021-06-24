CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio75_rh_grupos(
C_GRUPORH                       STRING,
C_USU_RESP                      STRING,
C_NEGOCIORH                     STRING,
D_GRUPO                         STRING,
C_MODO                          STRING,
I_COMISIONA                     STRING,
F_ALTA                          STRING,
F_BAJA                          STRING,
I_ACTIVO                        STRING,
I_AUSENTISMO                    STRING

)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio75/dim/rh_grupos';