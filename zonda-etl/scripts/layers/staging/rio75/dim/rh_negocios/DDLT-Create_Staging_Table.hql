CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio75_rh_negocios(
C_NEGOCIORH                 STRING,
N_NEGOCIO_RIO3              STRING,
D_NEGOCIO                   STRING,
C_USU_RESP                  STRING,
C_DEPARTAMENTORH            STRING,
F_ALTA                      STRING,
F_BAJA                      STRING,
I_ACTIVO                    STRING,
I_LIQUIDABLE                STRING,
N_ASEGURADO                 STRING,
I_CII                       STRING,
C_TIPO                      STRING

)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio75/dim/rh_negocios';