CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_staging.gsa_v_posicion_custodia(
BRANCH                                  STRING,
FECHAPOS                                STRING,
CNO              		                STRING,
NUM_ATIT                                STRING,
SECID                                   STRING,
LUGAR                                   STRING,
ESTADO                                  STRING,
SUBESTADO                               STRING,
SUBESTADO2                              STRING,
NOMINALES                               STRING
)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/staging/gsa/fact/v_posicion_custodia';