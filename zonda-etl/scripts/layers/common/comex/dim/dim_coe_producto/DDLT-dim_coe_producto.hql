CREATE EXTERNAL TABLE IF NOT EXISTS BI_CORP_COMMON.DIM_COE_PRODUCTO (
COD_COE_PRODUCTO		INT,
COD_COE_SUBPRODUCTO		STRING,
DS_COE_DESCRIPCION		STRING
)
PARTITIONED BY (PARTITION_DATE STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/comex/dim/dim_coe_producto'