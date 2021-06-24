CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_per_mails(
cod_per_nup	string	
, ds_per_secuencia	int	
, ds_per_mail	string	
, ds_per_fuente	string	
, ds_per_sec_fuente	string	
, ds_per_estado	string	
, dt_per_fecha_alta	string	
, dt_per_fecha_modificacion	string	
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
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/personas/dim/dim_per_mails'