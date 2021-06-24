CREATE EXTERNAL TABLE IF NOT EXISTS bi_corp_common.dim_rec_info_requerida_valores_hijos ( 
	cod_rec_entidad string
	, cod_rec_info_reque_p int
	, cod_rec_valor_p string
	, cod_rec_nro_valor_p int
	, cod_rec_info_reque_h int
	, cod_rec_valor_h string
	, cod_rec_nro_valor_h int
	, ds_rec_desc_valor string
	, cod_rec_estado_valor string
	, cod_rec_valor_info_h string
	, cod_rec_valor_info_p string
	, partition_date string)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat' 
LOCATION '${DATA_LAKE_SERVER}/santander/bi-corp/common/reclamos/dim_rec_info_requerida_valores_hijos';