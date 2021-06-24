CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_suc_sucursales(
    cod_suc_sucursal string,
    des_suc_sucursal string,
    des_suc_localidad string,
    cod_suc_tiposucursal string,
    dt_suc_fechaalta string,
    dt_suc_fechabaja string)
PARTITIONED BY (
    partition_date string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/dim/dim_suc_sucursales';