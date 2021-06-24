CREATE EXTERNAL TABLE IF NOT EXISTS BI_CORP_COMMON.DIM_OBP_TRANSACCIONES (
COD_OBP_TRANSACCION                 STRING,
DS_OBP_DESCRIPCION                  STRING
)
PARTITIONED BY (PARTITION_DATE STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/obp/dim/dim_obp_transacciones'