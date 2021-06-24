CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_producto_marca
(
cod_ren_producto STRING,
cod_marca STRING

)
PARTITIONED BY (
    partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
'${DATA_LAKE_SERVER}/santander/bi-corp/common/rentabilidad/dim/dim_producto_marca';