CREATE EXTERNAL TABLE IF NOT EXISTS BI_CORP_COMMON.DIM_COE_DIVISA (
COD_BCRA_DIVISA		STRING,
COD_DIVISA		STRING,
DESCRIPCION_DIVISA	STRING,
COD_AFIP	STRING
)
PARTITIONED BY (PARTITION_INSERT INT)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/comex/dim/dim_coe_divisa'