CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_rec_actor (
    cod_rec_actor STRING,
	cod_rec_tipo_actor STRING,
	ds_rec_actor STRING,
	cod_rec_usuario_alta STRING,
	dt_rec_actor_alta TIMESTAMP,
	cod_rec_usuario_modif STRING,
	dt_rec_actor_modif TIMESTAMP,
	partition_date STRING )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/reclamos/dim_rec_actor';