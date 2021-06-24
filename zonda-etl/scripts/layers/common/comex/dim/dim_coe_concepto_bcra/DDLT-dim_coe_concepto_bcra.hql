CREATE EXTERNAL TABLE IF NOT EXISTS BI_CORP_COMMON.DIM_COE_CONCEPTO_BCRA (
COD_COE_BCRA	    STRING,
COD_COE_ALIAS		STRING,
DS_COE_DESCRIPCION	STRING,
DS_COE_EXTENDIDO	STRING,
COD_COE_AFIP	    STRING
)
PARTITIONED BY (PARTITION_INSERT INT)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/comex/dim/dim_coe_concepto_bcra'