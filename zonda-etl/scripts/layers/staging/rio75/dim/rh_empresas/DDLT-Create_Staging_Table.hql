CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.rio75_rh_empresas(
C_EMPRESA                   STRING,
D_EMPRESA                   STRING,
F_ALTA                      STRING,
F_BAJA                      STRING,
I_ACTIVO                    STRING,
D_DOMICILIO                 STRING,
C_USU_RESP                  STRING,
I_LIQUIDABLE                STRING,
C_HORAS_LABORALES           STRING,
C_HORAS_LACTANCIA           STRING


)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/rio75/dim/rh_empresas';