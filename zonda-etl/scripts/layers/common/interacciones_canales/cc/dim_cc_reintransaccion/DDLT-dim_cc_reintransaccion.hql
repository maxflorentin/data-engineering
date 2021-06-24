CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_cc_reintransacciones(
COD_CC_NOMBRE                       STRING,
DS_CC_NOMBRE                        STRING,
FLAG_CC_ROBUSTECIDA                 INT
)
PARTITIONED BY (PARTITION_DATE string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/interacciones_canales/cc/dim/dim_cc_reintransacciones';