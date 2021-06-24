CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_rec_canal (
  cod_rec_canal_cosmos INT,
  cod_rec_canal STRING, 
  ds_rec_canal STRING, 
  cod_rec_usuario_alta STRING, 
  dt_rec_canal_alta TIMESTAMP, 
  cod_rec_usuario_modif STRING, 
  dt_rec_canal_modif TIMESTAMP, 
  dt_rec_canal_baja TIMESTAMP, 
  partition_date STRING )
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  '${DATA_LAKE_SERVER}/santander/bi-corp/common/reclamos/dim_rec_canal';