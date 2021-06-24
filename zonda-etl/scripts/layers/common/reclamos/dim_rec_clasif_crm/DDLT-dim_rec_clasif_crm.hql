CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_rec_clasif_crm ( 
	cod_rec_entidad string , 
	cod_rec_tipo_persona string , 
	cod_rec_crm string , 
	ds_rec_crm string , 
	cod_rec_clasif_select int , 
	partition_date string ) 
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' 
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/reclamos/dim_rec_clasif_crm';