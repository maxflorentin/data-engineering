CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_pro_producto(
    cod_pro_claveproducto string,
    cod_pro_producto string,
    cod_pro_subproducto string,
    ds_pro_producto string)
PARTITIONED BY (
    partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/dim/dim_pro_producto';