CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_areanegocio_marca
(
cod_ren_area_negocio STRING,
cod_adn_n1 STRING,
ds_adn_n1 STRING,
cod_adn_n2 STRING,
ds_adn_n2 STRING,
cod_adn_n3 STRING,
ds_adn_n3 STRING,
cod_adn_n4 STRING,
ds_adn_n4 STRING
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
'${DATA_LAKE_SERVER}/santander/bi-corp/common/rentabilidad/dim/dim_areanegocio_marca';