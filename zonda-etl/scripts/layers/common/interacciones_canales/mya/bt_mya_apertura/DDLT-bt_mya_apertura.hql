CREATE EXTERNAL TABLE IF NOT EXISTS BI_CORP_COMMON.BT_MYA_APERTURA (
COD_MYA_MENSAJE                           STRING,
COD_PER_NUP                         BIGINT,
DS_MYA_NOVEDAD                            STRING,
COD_MYA_ACCION                           STRING,
COD_MYA_DESTINO                            STRING,
TS_MYA_FECHA                            STRING
)
PARTITIONED BY (PARTITION_DATE STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/mya/fact/bt_mya_apertura'