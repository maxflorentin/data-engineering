CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_rec_concepto_sac ( 
	cod_rec_entidad string , 
	cod_rec_concepto_sac int , 
	ds_rec_concepto_sac string , 
	cod_rec_tipo_reclamo_sac string , 
	cod_rec_estado_concepto_sac string , 
	partition_date string ) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' 
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/reclamos/dim_rec_concepto_sac';