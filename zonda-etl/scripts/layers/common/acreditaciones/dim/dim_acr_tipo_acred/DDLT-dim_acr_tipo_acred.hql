CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_acr_tipo_acred (
    cod_acr_sistema             string,
    ds_acr_sistema              string,
    cod_acr_tipo_acred          string,
    ds_acr_tipo_acred           string
)
PARTITIONED BY (
    partition_date          string)

ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/acreditaciones/dim/dim_acr_tipo_acred';
