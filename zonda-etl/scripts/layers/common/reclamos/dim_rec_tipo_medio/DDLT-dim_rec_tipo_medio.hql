CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_rec_tipo_medio ( 
	cod_rec_tipo_medio int ,
	ds_rec_tipo_medio string ,
	ds_rec_detall_tipo_medio string ,
	flag_rec_tipo_medio string ,
	cod_rec_est_medio string ,
	ds_rec_sector_owner_medio string ,
	flag_rec_visible_medio string ,
	cod_rec_mjs_asoc_medio string ,
	partition_date string ) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' 
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/reclamos/dim_rec_tipo_medio';